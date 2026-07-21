#ifndef ORCHFS_KERNELFS_SPDK_NVME_BACKEND_HPP
#define ORCHFS_KERNELFS_SPDK_NVME_BACKEND_HPP

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <system_error>
#include <boost/container/small_vector.hpp>

namespace orchfs::nvme {

enum class Operation {
    read,
    write,
};

// One DMA command in an I/O plan.  device_* describes the aligned NVMe
// transfer.  The user bytes live at bounce_offset in that transfer and at
// user_offset in the caller's buffer.
struct IoSegment {
    std::uint64_t device_offset{};
    std::size_t device_length{};
    std::size_t user_offset{};
    std::size_t user_length{};
    std::size_t bounce_offset{};
    bool read_before_write{};

    friend bool operator==(const IoSegment &, const IoSegment &) = default;
};

struct IoPlan {
    boost::container::small_vector<IoSegment, 4> segments;
    std::uint64_t physical_begin{};
    std::uint64_t physical_end{};
};

// Build an LBA-aligned plan without touching SPDK.  max_transfer_size is
// rounded down to an LBA multiple.  Partial-LBA writes are marked for RMW.
std::error_code plan_io(Operation operation,
                        std::uint64_t offset,
                        std::size_t length,
                        std::uint32_t lba_size,
                        std::uint32_t max_transfer_size,
                        IoPlan &out);

enum class BackendErrc {
    controller_busy = 1,
    controller_init_failed,
    namespace_not_found,
    wrong_poller_thread,
    poller_failed,
    stopping,
    not_stopped,
    reentrant_poll,
};

std::error_code make_error_code(BackendErrc error) noexcept;

// NVMe completion status is retained in the error value (packed SCT/SC plus
// one, so error_code::operator bool remains true).  The category distinguishes
// it from errno values.
std::error_code make_nvme_status_error(std::uint8_t status_code_type,
                                       std::uint8_t status_code) noexcept;
bool is_nvme_status_error(const std::error_code &error) noexcept;

namespace detail {

// A write ticket linearizes acceptance and keeps the complete physical LBA
// envelope reserved until every command of the logical write has completed.
// Keeping this hardware-independent also lets the ordering rules be unit
// tested without opening a controller.
struct WriteTicket final {
    std::uint64_t sequence{};
    std::uint64_t physical_begin{};
    std::uint64_t physical_end{};
    WriteTicket *previous{};
    WriteTicket *next{};
    bool granted{};
    bool linked{};
};

class WriteCoordinator final {
public:
    using Ticket = WriteTicket *;

    Ticket accept(WriteTicket &storage,
                  std::uint64_t physical_begin,
                  std::uint64_t physical_end,
                  std::error_code &error);

    // Acceptance order is enforced only for overlapping ranges, so disjoint
    // physical writes can still run concurrently on different pollers.
    bool try_grant(Ticket ticket) noexcept;
    void complete(Ticket ticket) noexcept;

    std::uint64_t capture_fence() const noexcept;
    bool fence_ready(std::uint64_t fence) const noexcept;

private:
    // Tickets live in the backend's preallocated Request objects. The short
    // intrusive-list critical sections replace per-write shared_ptr and
    // copy-on-write snapshot allocations.
    mutable std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
    WriteTicket *head_{};
    WriteTicket *tail_{};
    std::uint64_t last_sequence_{};
};

} // namespace detail

struct Config {
    std::string pci_bdf;
    std::uint32_t namespace_id{1};
    std::size_t poller_count{1};
    std::uint32_t queue_depth{32};
    std::size_t bounce_buffers_per_poller{32};

    // Zero uses the namespace maximum.  A non-zero value is capped by the
    // namespace maximum and rounded down to an LBA multiple.
    std::uint32_t max_transfer_size{1024U * 1024U};

    std::string application_name{"orchfs_kfs"};
    std::string reactor_mask{"0x1"};
    std::string hugepage_directory;
    int shared_memory_id{-1};
};

using CompletionCallback = void (*)(void *context,
                                    std::error_code error,
                                    std::size_t bytes);

struct PollResult {
    std::size_t completed_requests{};
    std::error_code error;
    bool stopped{};
};

// The KFS process owns one Backend. submit_* publishes to a lock-free MPSC
// inbox; all SPDK qpair operations happen from poll(). The first poll call
// binds that qpair permanently to the calling Runtime worker. Caller buffers
// must remain valid
// until their completion callback.  Failed split writes may already have
// changed earlier media ranges; bytes is reported as zero with the error.  The
// owner must complete the documented stop/poll/close sequence before destroy.
class SpdkNvmeBackend final {
public:
    static std::unique_ptr<SpdkNvmeBackend> open(const Config &config,
                                                 std::error_code &error);

    ~SpdkNvmeBackend();

    SpdkNvmeBackend(const SpdkNvmeBackend &) = delete;
    SpdkNvmeBackend &operator=(const SpdkNvmeBackend &) = delete;
    SpdkNvmeBackend(SpdkNvmeBackend &&) = delete;
    SpdkNvmeBackend &operator=(SpdkNvmeBackend &&) = delete;

    std::error_code submit_read(std::size_t poller_id,
                                std::uint64_t offset,
                                std::span<std::byte> destination,
                                CompletionCallback callback,
                                void *callback_context);
    std::error_code submit_write(std::size_t poller_id,
                                 std::uint64_t offset,
                                 std::span<const std::byte> source,
                                 CompletionCallback callback,
                                 void *callback_context);
    // The backend captures a global write fence at submission. The namespace
    // FLUSH is not issued until every complete logical write accepted through
    // that fence has finished on every poller.
    std::error_code submit_flush(std::size_t poller_id,
                                 CompletionCallback callback,
                                 void *callback_context);

    // Register externally shared hugepage memory for direct NVMe DMA. The
    // caller must keep the region mapped and unregister it only after all I/O
    // referencing it has completed.
    std::error_code register_memory(void *address, std::size_t length) noexcept;
    std::error_code unregister_memory(void *address,
                                      std::size_t length) noexcept;

    // max_completions == 0 asks SPDK to consume all currently available
    // completions.  Completion callbacks execute on this poller thread.
    PollResult poll(std::size_t poller_id,
                    std::uint32_t max_completions = 0);

    // Stop is two phase: reject new submissions, keep polling each poller until
    // PollResult::stopped, then close() detaches the controller.
    void request_stop() noexcept;
    bool stop_requested() const noexcept;
    bool poller_stopped(std::size_t poller_id) const noexcept;
    std::error_code close() noexcept;

    std::size_t poller_count() const noexcept;
    std::uint32_t lba_size() const noexcept;
    std::uint32_t max_transfer_size() const noexcept;
    std::uint64_t capacity_bytes() const noexcept;

    static constexpr bool compiled_with_spdk() noexcept {
#ifdef ORCHFS_HAS_SPDK
        return true;
#else
        return false;
#endif
    }

private:
    struct Impl;
    explicit SpdkNvmeBackend(std::unique_ptr<Impl> impl) noexcept;

    std::unique_ptr<Impl> impl_;
};

} // namespace orchfs::nvme

namespace std {
template <>
struct is_error_code_enum<orchfs::nvme::BackendErrc> : true_type {};
} // namespace std

#endif // ORCHFS_KERNELFS_SPDK_NVME_BACKEND_HPP
