#include "spdk_nvme_backend.hpp"
#include "thread_affinity_guard.hpp"

#include <algorithm>
#include <atomic>
#include <bit>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <limits>
#include <list>
#include <mutex>
#include <new>
#include <sstream>
#include <thread>
#include <utility>

#ifdef ORCHFS_HAS_SPDK
#include <spdk/env.h>
#include <spdk/nvme.h>
#endif

namespace orchfs::nvme {
namespace {

class BackendErrorCategory final : public std::error_category {
public:
    const char *name() const noexcept override { return "orchfs.spdk"; }

    std::string message(int value) const override {
        switch (static_cast<BackendErrc>(value)) {
        case BackendErrc::controller_busy:
            return "an SPDK NVMe controller is already owned by this process";
        case BackendErrc::controller_init_failed:
            return "SPDK NVMe controller initialization failed";
        case BackendErrc::namespace_not_found:
            return "NVMe namespace does not exist or is inactive";
        case BackendErrc::wrong_poller_thread:
            return "NVMe poller was called from a different thread";
        case BackendErrc::poller_failed:
            return "NVMe poller failed";
        case BackendErrc::stopping:
            return "NVMe backend is stopping and rejects new requests";
        case BackendErrc::not_stopped:
            return "NVMe pollers have not drained and stopped";
        case BackendErrc::reentrant_poll:
            return "NVMe poller cannot be entered recursively";
        }
        return "unknown OrchFS SPDK error";
    }
};

class NvmeStatusCategory final : public std::error_category {
public:
    const char *name() const noexcept override { return "orchfs.nvme_status"; }

    std::string message(int value) const override {
        const unsigned packed = value > 0 ? static_cast<unsigned>(value - 1) : 0;
        const unsigned sct = (packed >> 8U) & 0xffU;
        const unsigned sc = packed & 0xffU;
        std::ostringstream stream;
        stream << "NVMe completion error (sct=" << sct << ", sc=" << sc << ')';
        return stream.str();
    }
};

const std::error_category &backend_category() noexcept {
    static BackendErrorCategory category;
    return category;
}

const std::error_category &nvme_status_category() noexcept {
    static NvmeStatusCategory category;
    return category;
}

std::error_code invalid_argument() noexcept {
    return std::make_error_code(std::errc::invalid_argument);
}

} // namespace

std::error_code make_error_code(BackendErrc error) noexcept {
    return {static_cast<int>(error), backend_category()};
}

std::error_code make_nvme_status_error(std::uint8_t status_code_type,
                                       std::uint8_t status_code) noexcept {
    const auto packed = (static_cast<unsigned>(status_code_type) << 8U) |
                        static_cast<unsigned>(status_code);
    // Add one so even an accidentally supplied successful (0, 0) status still
    // represents an error_code with a true boolean value.
    return {static_cast<int>(packed + 1U), nvme_status_category()};
}

bool is_nvme_status_error(const std::error_code &error) noexcept {
    return error.category() == nvme_status_category();
}

std::error_code plan_io(Operation operation,
                        std::uint64_t offset,
                        std::size_t length,
                        std::uint32_t lba_size,
                        std::uint32_t max_transfer_size,
                        IoPlan &out) {
    out = {};
    if (lba_size == 0 || max_transfer_size < lba_size) {
        return invalid_argument();
    }

    const std::uint64_t max_transfer =
        (static_cast<std::uint64_t>(max_transfer_size) / lba_size) * lba_size;
    if (max_transfer == 0) {
        return invalid_argument();
    }
    if (length > std::numeric_limits<std::uint64_t>::max() - offset) {
        return std::make_error_code(std::errc::value_too_large);
    }

    const std::uint64_t logical_end = offset + static_cast<std::uint64_t>(length);
    if (length == 0) {
        out.physical_begin = offset;
        out.physical_end = offset;
        return {};
    }
    out.physical_begin = offset - (offset % lba_size);

    const std::uint64_t end_remainder = logical_end % lba_size;
    if (end_remainder != 0) {
        const std::uint64_t padding = lba_size - end_remainder;
        if (logical_end > std::numeric_limits<std::uint64_t>::max() - padding) {
            out = {};
            return std::make_error_code(std::errc::value_too_large);
        }
        out.physical_end = logical_end + padding;
    } else {
        out.physical_end = logical_end;
    }

    auto append = [&](std::uint64_t device_offset,
                      std::uint64_t device_length,
                      std::uint64_t logical_begin,
                      std::uint64_t segment_logical_end) {
        const bool partial = logical_begin != device_offset ||
                             segment_logical_end != device_offset + device_length;
        out.segments.push_back(IoSegment{
            .device_offset = device_offset,
            .device_length = static_cast<std::size_t>(device_length),
            .user_offset = static_cast<std::size_t>(logical_begin - offset),
            .user_length = static_cast<std::size_t>(segment_logical_end - logical_begin),
            .bounce_offset = static_cast<std::size_t>(logical_begin - device_offset),
            .read_before_write = operation == Operation::write && partial,
        });
    };

    try {
        std::uint64_t cursor = offset;

        // Keep a partial head to one LBA.  This avoids reading an entire
        // max-transfer-sized window just to preserve a few leading bytes.
        if (cursor % lba_size != 0) {
            const std::uint64_t device_offset = cursor - (cursor % lba_size);
            const std::uint64_t device_end = device_offset + lba_size;
            const std::uint64_t segment_end = std::min(logical_end, device_end);
            append(device_offset, lba_size, cursor, segment_end);
            cursor = segment_end;
        }

        const std::uint64_t aligned_logical_end =
            logical_end - (logical_end % lba_size);
        while (cursor < aligned_logical_end) {
            const std::uint64_t remaining = aligned_logical_end - cursor;
            const std::uint64_t chunk = std::min(remaining, max_transfer);
            append(cursor, chunk, cursor, cursor + chunk);
            cursor += chunk;
        }

        if (cursor < logical_end) {
            append(cursor, lba_size, cursor, logical_end);
        }
    } catch (const std::bad_alloc &) {
        out = {};
        return std::make_error_code(std::errc::not_enough_memory);
    }

    return {};
}

detail::WriteCoordinator::Ticket detail::WriteCoordinator::accept(
    std::uint64_t physical_begin,
    std::uint64_t physical_end,
    std::error_code &error) {
    error.clear();
    if (physical_begin >= physical_end) {
        error = invalid_argument();
        return {};
    }

    Ticket ticket;
    try {
        ticket = std::make_shared<WriteTicket>();
        std::lock_guard lock(mutex_);
        if (last_sequence_ == std::numeric_limits<std::uint64_t>::max()) {
            error = std::make_error_code(std::errc::value_too_large);
            return {};
        }
        ticket->sequence = last_sequence_ + 1;
        ticket->physical_begin = physical_begin;
        ticket->physical_end = physical_end;
        outstanding_.push_back(ticket);
        last_sequence_ = ticket->sequence;
    } catch (const std::bad_alloc &) {
        error = std::make_error_code(std::errc::not_enough_memory);
        return {};
    }
    return ticket;
}

bool detail::WriteCoordinator::try_grant(const Ticket &ticket) noexcept {
    if (!ticket) {
        return false;
    }
    std::lock_guard lock(mutex_);
    for (const Ticket &earlier : outstanding_) {
        if (earlier == ticket) {
            ticket->granted = true;
            return true;
        }
        const bool overlaps =
            earlier->physical_begin < ticket->physical_end &&
            ticket->physical_begin < earlier->physical_end;
        if (overlaps) {
            return false;
        }
    }
    return false;
}

void detail::WriteCoordinator::complete(const Ticket &ticket) noexcept {
    if (!ticket) {
        return;
    }
    std::lock_guard lock(mutex_);
    for (auto iterator = outstanding_.begin(); iterator != outstanding_.end();
         ++iterator) {
        if (*iterator == ticket) {
            ticket->granted = false;
            outstanding_.erase(iterator);
            return;
        }
    }
}

std::uint64_t detail::WriteCoordinator::capture_fence() const noexcept {
    std::lock_guard lock(mutex_);
    return last_sequence_;
}

bool detail::WriteCoordinator::fence_ready(std::uint64_t fence) const noexcept {
    std::lock_guard lock(mutex_);
    for (const Ticket &ticket : outstanding_) {
        if (ticket->sequence <= fence) {
            return false;
        }
    }
    return true;
}

#ifdef ORCHFS_HAS_SPDK
namespace {

std::mutex global_backend_mutex;
bool global_backend_active = false;
bool global_env_initialized_once = false;

enum class RequestKind {
    read,
    write,
    flush,
};

enum class RequestStage {
    ready,
    read_inflight,
    rmw_read_inflight,
    rmw_write_ready,
    write_inflight,
    flush_inflight,
    done,
};

struct PollerState;

struct Request {
    RequestKind kind{};
    RequestStage stage{RequestStage::ready};
    IoPlan plan;
    std::size_t segment_index{};
    std::byte *read_buffer{};
    const std::byte *write_buffer{};
    std::size_t requested_bytes{};
    CompletionCallback callback{};
    void *callback_context{};
    std::error_code error;
    void *bounce{};
    PollerState *poller{};
    detail::WriteCoordinator::Ticket write_ticket;
    std::uint64_t write_fence{};
};

class DmaBouncePool {
public:
    std::error_code initialize(std::size_t count,
                               std::size_t buffer_size,
                               std::size_t lba_size) {
        try {
            all_.reserve(count);
            free_.reserve(count);
        } catch (const std::bad_alloc &) {
            return std::make_error_code(std::errc::not_enough_memory);
        }

        for (std::size_t index = 0; index < count; ++index) {
            // SPDK requires a power-of-two allocation alignment.  bit_ceil
            // covers every namespace LBA without assuming a 4KiB sector.
            const std::size_t alignment =
                std::bit_ceil(std::max<std::size_t>(lba_size, 64));
            void *buffer = spdk_dma_zmalloc(buffer_size, alignment, nullptr);
            if (buffer == nullptr) {
                release_all();
                return std::make_error_code(std::errc::not_enough_memory);
            }
            all_.push_back(buffer);
            free_.push_back(buffer);
        }
        return {};
    }

    void *acquire() noexcept {
        if (free_.empty()) {
            return nullptr;
        }
        void *buffer = free_.back();
        free_.pop_back();
        return buffer;
    }

    void release(void *buffer) noexcept {
        if (buffer != nullptr) {
            free_.push_back(buffer);
        }
    }

    void release_all() noexcept {
        for (void *buffer : all_) {
            spdk_free(buffer);
        }
        all_.clear();
        free_.clear();
    }

    ~DmaBouncePool() { release_all(); }

private:
    std::vector<void *> all_;
    std::vector<void *> free_;
};

struct PollerState {
    mutable std::mutex incoming_mutex;
    std::list<std::unique_ptr<Request>> incoming;
    std::list<std::unique_ptr<Request>> active;

    mutable std::mutex owner_mutex;
    bool owner_bound{};
    bool polling{};
    std::thread::id owner;

    struct spdk_nvme_qpair *qpair{};
    DmaBouncePool bounce_pool;
    bool initialized{};
    std::atomic<bool> stopped{false};
    std::atomic<bool> failed{false};
};

void release_bounce(Request &request) noexcept {
    if (request.bounce != nullptr) {
        request.poller->bounce_pool.release(request.bounce);
        request.bounce = nullptr;
    }
}

void finish_segment(Request &request) noexcept {
    release_bounce(request);
    ++request.segment_index;
    request.stage = request.segment_index == request.plan.segments.size()
                        ? RequestStage::done
                        : RequestStage::ready;
}

void fail_request(Request &request, std::error_code error) noexcept {
    release_bounce(request);
    request.error = error;
    request.stage = RequestStage::done;
}

void command_complete(void *argument, const struct spdk_nvme_cpl *completion) {
    auto &request = *static_cast<Request *>(argument);
    if (spdk_nvme_cpl_is_error(completion)) {
        fail_request(request,
                     make_nvme_status_error(completion->status.sct,
                                            completion->status.sc));
        return;
    }

    switch (request.stage) {
    case RequestStage::read_inflight: {
        const IoSegment &segment = request.plan.segments[request.segment_index];
        std::memcpy(request.read_buffer + segment.user_offset,
                    static_cast<std::byte *>(request.bounce) + segment.bounce_offset,
                    segment.user_length);
        finish_segment(request);
        break;
    }
    case RequestStage::rmw_read_inflight: {
        const IoSegment &segment = request.plan.segments[request.segment_index];
        std::memcpy(static_cast<std::byte *>(request.bounce) + segment.bounce_offset,
                    request.write_buffer + segment.user_offset,
                    segment.user_length);
        request.stage = RequestStage::rmw_write_ready;
        break;
    }
    case RequestStage::write_inflight:
        finish_segment(request);
        break;
    case RequestStage::flush_inflight:
        request.stage = RequestStage::done;
        break;
    case RequestStage::ready:
    case RequestStage::rmw_write_ready:
    case RequestStage::done:
        fail_request(request, make_error_code(BackendErrc::poller_failed));
        break;
    }
}

std::error_code error_from_negative_rc(int rc) noexcept {
    if (rc >= 0) {
        return {};
    }
    return {-rc, std::generic_category()};
}

} // namespace

struct SpdkNvmeBackend::Impl {
    Config config;
    struct spdk_nvme_ctrlr *controller{};
    struct spdk_nvme_ns *namespace_handle{};
    std::uint32_t lba{};
    std::uint32_t max_transfer{};
    std::uint64_t capacity{};
    std::vector<std::unique_ptr<PollerState>> pollers;
    std::atomic<bool> accepting{true};
    std::atomic<bool> stopping{false};
    std::atomic<bool> closed{false};
    bool environment_started{};
    detail::WriteCoordinator writes;

    std::error_code initialize_poller(PollerState &poller) {
        struct spdk_nvme_io_qpair_opts options {};
        spdk_nvme_ctrlr_get_default_io_qpair_opts(controller, &options, sizeof(options));
        options.io_queue_size = config.queue_depth;
        options.io_queue_requests = std::max(options.io_queue_requests, config.queue_depth);
        poller.qpair =
            spdk_nvme_ctrlr_alloc_io_qpair(controller, &options, sizeof(options));
        if (poller.qpair == nullptr) {
            return make_error_code(BackendErrc::poller_failed);
        }

        std::error_code error = poller.bounce_pool.initialize(
            config.bounce_buffers_per_poller, max_transfer, lba);
        if (error) {
            spdk_nvme_ctrlr_free_io_qpair(poller.qpair);
            poller.qpair = nullptr;
            return error;
        }
        poller.initialized = true;
        return {};
    }

    bool enter_poller(PollerState &poller, std::error_code &error) {
        const std::thread::id current = std::this_thread::get_id();
        std::lock_guard lock(poller.owner_mutex);
        if (!poller.owner_bound) {
            poller.owner = current;
            poller.owner_bound = true;
        } else if (poller.owner != current) {
            error = make_error_code(BackendErrc::wrong_poller_thread);
            return false;
        }
        if (poller.polling) {
            error = make_error_code(BackendErrc::reentrant_poll);
            return false;
        }
        poller.polling = true;
        return true;
    }

    void leave_poller(PollerState &poller) noexcept {
        std::lock_guard lock(poller.owner_mutex);
        poller.polling = false;
    }

    int process_completions(PollerState &poller,
                            std::uint32_t max_completions,
                            std::error_code &error) {
        const int result =
            spdk_nvme_qpair_process_completions(poller.qpair, max_completions);
        if (result < 0) {
            error = error_from_negative_rc(result);
            if (!error) {
                error = make_error_code(BackendErrc::poller_failed);
            }
            poller.failed.store(true, std::memory_order_release);
        }
        return result;
    }

    template <typename Submit>
    int submit_with_retry(PollerState &poller,
                          Submit &&submit,
                          std::error_code &poll_error) {
        int result = submit();
        if (result != -ENOMEM) {
            return result;
        }

        // SPDK uses -ENOMEM for request-pool pressure.  Reap completions and
        // retry instead of surfacing a transient allocation failure.
        if (process_completions(poller, 0, poll_error) < 0) {
            return -EIO;
        }
        return submit();
    }

    void drive_request(PollerState &poller,
                       Request &request,
                       std::error_code &poll_error) {
        if (request.stage == RequestStage::done ||
            request.stage == RequestStage::read_inflight ||
            request.stage == RequestStage::rmw_read_inflight ||
            request.stage == RequestStage::write_inflight ||
            request.stage == RequestStage::flush_inflight) {
            return;
        }

        if (request.kind == RequestKind::flush) {
            if (!writes.fence_ready(request.write_fence)) {
                return;
            }
            const int result = submit_with_retry(
                poller,
                [&] {
                    return spdk_nvme_ns_cmd_flush(namespace_handle,
                                                  poller.qpair,
                                                  command_complete,
                                                  &request);
                },
                poll_error);
            if (result == 0) {
                request.stage = RequestStage::flush_inflight;
            } else if (result != -ENOMEM) {
                fail_request(request, error_from_negative_rc(result));
            }
            return;
        }

        if (request.kind == RequestKind::write && request.write_ticket &&
            !writes.try_grant(request.write_ticket)) {
            return;
        }

        if (request.segment_index >= request.plan.segments.size()) {
            request.stage = RequestStage::done;
            return;
        }

        const IoSegment &segment = request.plan.segments[request.segment_index];
        if (request.bounce == nullptr) {
            request.bounce = poller.bounce_pool.acquire();
            if (request.bounce == nullptr) {
                return;
            }
        }

        const std::uint64_t lba_offset = segment.device_offset / lba;
        const std::uint32_t lba_count =
            static_cast<std::uint32_t>(segment.device_length / lba);

        if (request.kind == RequestKind::read) {
            const int result = submit_with_retry(
                poller,
                [&] {
                    return spdk_nvme_ns_cmd_read(namespace_handle,
                                                 poller.qpair,
                                                 request.bounce,
                                                 lba_offset,
                                                 lba_count,
                                                 command_complete,
                                                 &request,
                                                 0);
                },
                poll_error);
            if (result == 0) {
                request.stage = RequestStage::read_inflight;
            } else if (result != -ENOMEM) {
                fail_request(request, error_from_negative_rc(result));
            }
            return;
        }

        if (request.stage == RequestStage::ready && segment.read_before_write) {
            const int result = submit_with_retry(
                poller,
                [&] {
                    return spdk_nvme_ns_cmd_read(namespace_handle,
                                                 poller.qpair,
                                                 request.bounce,
                                                 lba_offset,
                                                 lba_count,
                                                 command_complete,
                                                 &request,
                                                 0);
                },
                poll_error);
            if (result == 0) {
                request.stage = RequestStage::rmw_read_inflight;
            } else if (result != -ENOMEM) {
                fail_request(request, error_from_negative_rc(result));
            }
            return;
        }

        if (request.stage == RequestStage::ready) {
            std::memcpy(request.bounce,
                        request.write_buffer + segment.user_offset,
                        segment.user_length);
        }

        const int result = submit_with_retry(
            poller,
            [&] {
                return spdk_nvme_ns_cmd_write(namespace_handle,
                                              poller.qpair,
                                              request.bounce,
                                              lba_offset,
                                              lba_count,
                                              command_complete,
                                              &request,
                                              0);
            },
            poll_error);
        if (result == 0) {
            request.stage = RequestStage::write_inflight;
        } else if (result != -ENOMEM) {
            fail_request(request, error_from_negative_rc(result));
        }
    }

    void fail_all(PollerState &poller, const std::error_code &error) {
        for (auto &request : poller.active) {
            fail_request(*request, error);
        }
    }

    // Keep Request objects alive while freeing a failed qpair: SPDK aborts
    // queued commands during free and may invoke their completion callbacks.
    void fail_poller(PollerState &poller, const std::error_code &error) {
        poller.failed.store(true, std::memory_order_release);
        if (poller.qpair != nullptr) {
            spdk_nvme_ctrlr_free_io_qpair(poller.qpair);
            poller.qpair = nullptr;
        }
        poller.initialized = false;
        fail_all(poller, error);
    }

    std::size_t finish_requests(PollerState &poller) {
        std::size_t completed = 0;
        for (auto iterator = poller.active.begin(); iterator != poller.active.end();) {
            Request &request = **iterator;
            if (request.stage != RequestStage::done) {
                ++iterator;
                continue;
            }

            CompletionCallback callback = request.callback;
            void *context = request.callback_context;
            const std::error_code error = request.error;
            const std::size_t bytes = error ? 0 : request.requested_bytes;
            writes.complete(request.write_ticket);
            iterator = poller.active.erase(iterator);
            ++completed;
            callback(context, error, bytes);
        }
        return completed;
    }

    bool no_pending_work(PollerState &poller) {
        if (!poller.active.empty()) {
            return false;
        }
        std::lock_guard lock(poller.incoming_mutex);
        return poller.incoming.empty();
    }

    void stop_poller(PollerState &poller) noexcept {
        if (poller.qpair != nullptr) {
            spdk_nvme_ctrlr_free_io_qpair(poller.qpair);
            poller.qpair = nullptr;
        }
        poller.bounce_pool.release_all();
        poller.initialized = false;
        poller.stopped.store(true, std::memory_order_release);
    }
};

#else

struct SpdkNvmeBackend::Impl {};

#endif // ORCHFS_HAS_SPDK

SpdkNvmeBackend::SpdkNvmeBackend(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

SpdkNvmeBackend::~SpdkNvmeBackend() {
    if (impl_ != nullptr) {
        request_stop();
        (void)close();
    }
}

std::unique_ptr<SpdkNvmeBackend>
SpdkNvmeBackend::open(const Config &config, std::error_code &error) {
    error.clear();
#ifndef ORCHFS_HAS_SPDK
    (void)config;
    error = std::make_error_code(std::errc::not_supported);
    return nullptr;
#else
    if (config.pci_bdf.empty() || config.namespace_id == 0 ||
        config.poller_count == 0 || config.queue_depth == 0 ||
        config.bounce_buffers_per_poller == 0 || config.application_name.empty() ||
        config.reactor_mask.empty()) {
        error = invalid_argument();
        return nullptr;
    }

    struct spdk_pci_addr allowed_address {};
    if (spdk_pci_addr_parse(&allowed_address, config.pci_bdf.c_str()) != 0) {
        error = invalid_argument();
        return nullptr;
    }

    bool reinitialize_environment = false;
    {
        std::lock_guard lock(global_backend_mutex);
        if (global_backend_active) {
            error = make_error_code(BackendErrc::controller_busy);
            return nullptr;
        }
        global_backend_active = true;
        reinitialize_environment = global_env_initialized_once;
    }

    auto release_global_claim = [] {
        std::lock_guard lock(global_backend_mutex);
        global_backend_active = false;
    };

    std::error_code affinity_error;
    detail::ThreadAffinityGuard affinity_guard(affinity_error);
    if (affinity_error) {
        release_global_claim();
        error = affinity_error;
        return nullptr;
    }

    int result = 0;
    if (reinitialize_environment) {
        result = spdk_env_init(nullptr);
    } else {
        struct spdk_env_opts options {};
        // SPDK v26.01 preserves the caller-provided ABI size in
        // spdk_env_opts_init(), so it must be populated before initialization.
        options.opts_size = sizeof(options);
        spdk_env_opts_init(&options);
        options.name = config.application_name.c_str();
        options.core_mask = config.reactor_mask.c_str();
        options.num_pci_addr = 1;
        options.pci_allowed = &allowed_address;
        if (!config.hugepage_directory.empty()) {
            options.hugedir = config.hugepage_directory.c_str();
        }
        if (config.shared_memory_id >= 0) {
            options.shm_id = config.shared_memory_id;
        }
        result = spdk_env_init(&options);
    }
    affinity_error = affinity_guard.restore();
    if (affinity_error) {
        if (result >= 0) {
            spdk_env_fini();
        }
        release_global_claim();
        error = affinity_error;
        return nullptr;
    }
    if (result < 0) {
        release_global_claim();
        error = error_from_negative_rc(result);
        return nullptr;
    }
    {
        std::lock_guard lock(global_backend_mutex);
        global_env_initialized_once = true;
    }

    struct spdk_nvme_transport_id transport {};
    transport.trtype = SPDK_NVME_TRANSPORT_PCIE;
    std::snprintf(transport.trstring, sizeof(transport.trstring), "%s", "PCIe");
    std::snprintf(transport.traddr,
                  sizeof(transport.traddr),
                  "%s",
                  config.pci_bdf.c_str());

    struct spdk_nvme_ctrlr *controller = spdk_nvme_connect(&transport, nullptr, 0);
    if (controller == nullptr) {
        spdk_env_fini();
        release_global_claim();
        error = make_error_code(BackendErrc::controller_init_failed);
        return nullptr;
    }

    struct spdk_nvme_ns *namespace_handle =
        spdk_nvme_ctrlr_get_ns(controller, config.namespace_id);
    if (namespace_handle == nullptr || !spdk_nvme_ns_is_active(namespace_handle)) {
        spdk_nvme_detach(controller);
        spdk_env_fini();
        release_global_claim();
        error = make_error_code(BackendErrc::namespace_not_found);
        return nullptr;
    }

    const std::uint32_t lba = spdk_nvme_ns_get_sector_size(namespace_handle);
    std::uint32_t max_transfer = spdk_nvme_ns_get_max_io_xfer_size(namespace_handle);
    if (config.max_transfer_size != 0) {
        max_transfer = std::min(max_transfer, config.max_transfer_size);
    }
    if (lba == 0) {
        spdk_nvme_detach(controller);
        spdk_env_fini();
        release_global_claim();
        error = invalid_argument();
        return nullptr;
    }
    max_transfer = (max_transfer / lba) * lba;
    if (max_transfer == 0) {
        spdk_nvme_detach(controller);
        spdk_env_fini();
        release_global_claim();
        error = invalid_argument();
        return nullptr;
    }

    try {
        auto impl = std::make_unique<Impl>();
        impl->config = config;
        impl->controller = controller;
        impl->namespace_handle = namespace_handle;
        impl->lba = lba;
        impl->max_transfer = max_transfer;
        impl->capacity = spdk_nvme_ns_get_size(namespace_handle);
        impl->environment_started = true;
        impl->pollers.reserve(config.poller_count);
        for (std::size_t index = 0; index < config.poller_count; ++index) {
            impl->pollers.push_back(std::make_unique<PollerState>());
        }
        return std::unique_ptr<SpdkNvmeBackend>(
            new SpdkNvmeBackend(std::move(impl)));
    } catch (const std::bad_alloc &) {
        spdk_nvme_detach(controller);
        spdk_env_fini();
        release_global_claim();
        error = std::make_error_code(std::errc::not_enough_memory);
        return nullptr;
    }
#endif
}

std::error_code SpdkNvmeBackend::submit_read(
    std::size_t poller_id,
    std::uint64_t offset,
    std::span<std::byte> destination,
    CompletionCallback callback,
    void *callback_context) {
#ifndef ORCHFS_HAS_SPDK
    (void)poller_id;
    (void)offset;
    (void)destination;
    (void)callback;
    (void)callback_context;
    return std::make_error_code(std::errc::not_supported);
#else
    if (impl_ == nullptr || callback == nullptr ||
        (destination.data() == nullptr && !destination.empty()) ||
        poller_id >= impl_->pollers.size()) {
        return invalid_argument();
    }
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::stopping);
    }
    PollerState &poller = *impl_->pollers[poller_id];
    if (poller.failed.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::poller_failed);
    }

    auto request = std::unique_ptr<Request>(new (std::nothrow) Request());
    if (!request) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    request->kind = RequestKind::read;
    request->read_buffer = destination.data();
    request->requested_bytes = destination.size();
    request->callback = callback;
    request->callback_context = callback_context;
    request->poller = &poller;
    std::error_code error = plan_io(Operation::read,
                                    offset,
                                    destination.size(),
                                    impl_->lba,
                                    impl_->max_transfer,
                                    request->plan);
    if (error) {
        return error;
    }
    if (!destination.empty() && request->plan.physical_end > impl_->capacity) {
        return std::make_error_code(std::errc::no_space_on_device);
    }

    try {
        std::lock_guard lock(poller.incoming_mutex);
        if (!impl_->accepting.load(std::memory_order_acquire)) {
            return make_error_code(BackendErrc::stopping);
        }
        poller.incoming.push_back(std::move(request));
    } catch (const std::bad_alloc &) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    return {};
#endif
}

std::error_code SpdkNvmeBackend::submit_write(
    std::size_t poller_id,
    std::uint64_t offset,
    std::span<const std::byte> source,
    CompletionCallback callback,
    void *callback_context) {
#ifndef ORCHFS_HAS_SPDK
    (void)poller_id;
    (void)offset;
    (void)source;
    (void)callback;
    (void)callback_context;
    return std::make_error_code(std::errc::not_supported);
#else
    if (impl_ == nullptr || callback == nullptr ||
        (source.data() == nullptr && !source.empty()) ||
        poller_id >= impl_->pollers.size()) {
        return invalid_argument();
    }
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::stopping);
    }
    PollerState &poller = *impl_->pollers[poller_id];
    if (poller.failed.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::poller_failed);
    }

    auto request = std::unique_ptr<Request>(new (std::nothrow) Request());
    if (!request) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    request->kind = RequestKind::write;
    request->write_buffer = source.data();
    request->requested_bytes = source.size();
    request->callback = callback;
    request->callback_context = callback_context;
    request->poller = &poller;
    std::error_code error = plan_io(Operation::write,
                                    offset,
                                    source.size(),
                                    impl_->lba,
                                    impl_->max_transfer,
                                    request->plan);
    if (error) {
        return error;
    }
    if (!source.empty() && request->plan.physical_end > impl_->capacity) {
        return std::make_error_code(std::errc::no_space_on_device);
    }

    detail::WriteCoordinator::Ticket write_ticket;
    if (!source.empty()) {
        write_ticket = impl_->writes.accept(request->plan.physical_begin,
                                            request->plan.physical_end,
                                            error);
        if (error) {
            return error;
        }
        request->write_ticket = write_ticket;
    }

    std::error_code enqueue_error;
    try {
        std::lock_guard lock(poller.incoming_mutex);
        if (!impl_->accepting.load(std::memory_order_acquire)) {
            enqueue_error = make_error_code(BackendErrc::stopping);
        } else {
            poller.incoming.push_back(std::move(request));
        }
    } catch (const std::bad_alloc &) {
        enqueue_error = std::make_error_code(std::errc::not_enough_memory);
    }
    if (enqueue_error) {
        impl_->writes.complete(write_ticket);
        return enqueue_error;
    }
    return {};
#endif
}

std::error_code SpdkNvmeBackend::submit_flush(
    std::size_t poller_id,
    CompletionCallback callback,
    void *callback_context) {
#ifndef ORCHFS_HAS_SPDK
    (void)poller_id;
    (void)callback;
    (void)callback_context;
    return std::make_error_code(std::errc::not_supported);
#else
    if (impl_ == nullptr || callback == nullptr || poller_id >= impl_->pollers.size()) {
        return invalid_argument();
    }
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::stopping);
    }
    PollerState &poller = *impl_->pollers[poller_id];
    if (poller.failed.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::poller_failed);
    }

    try {
        auto request = std::make_unique<Request>();
        request->kind = RequestKind::flush;
        request->callback = callback;
        request->callback_context = callback_context;
        request->poller = &poller;
        request->write_fence = impl_->writes.capture_fence();
        std::lock_guard lock(poller.incoming_mutex);
        if (!impl_->accepting.load(std::memory_order_acquire)) {
            return make_error_code(BackendErrc::stopping);
        }
        poller.incoming.push_back(std::move(request));
    } catch (const std::bad_alloc &) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    return {};
#endif
}

PollResult SpdkNvmeBackend::poll(std::size_t poller_id,
                                 std::uint32_t max_completions) {
#ifndef ORCHFS_HAS_SPDK
    (void)poller_id;
    (void)max_completions;
    return {.error = std::make_error_code(std::errc::not_supported)};
#else
    PollResult result;
    if (impl_ == nullptr || poller_id >= impl_->pollers.size()) {
        result.error = invalid_argument();
        return result;
    }

    PollerState &poller = *impl_->pollers[poller_id];
    if (poller.stopped.load(std::memory_order_acquire)) {
        result.stopped = true;
        return result;
    }
    if (!impl_->enter_poller(poller, result.error)) {
        return result;
    }
    struct PollExit final {
        Impl *impl;
        PollerState *poller;
        ~PollExit() { impl->leave_poller(*poller); }
    } poll_exit{impl_.get(), &poller};

    {
        std::lock_guard lock(poller.incoming_mutex);
        poller.active.splice(poller.active.end(), poller.incoming);
    }

    if (poller.failed.load(std::memory_order_acquire)) {
        result.error = make_error_code(BackendErrc::poller_failed);
        impl_->fail_all(poller, result.error);
        result.completed_requests += impl_->finish_requests(poller);
        if (impl_->stopping.load(std::memory_order_acquire)) {
            impl_->stop_poller(poller);
            result.stopped = true;
        }
        return result;
    }

    if (!poller.initialized) {
        if (impl_->stopping.load(std::memory_order_acquire) && poller.active.empty()) {
            impl_->stop_poller(poller);
            result.stopped = true;
            return result;
        }
        result.error = impl_->initialize_poller(poller);
        if (result.error) {
            impl_->fail_poller(poller, result.error);
            result.completed_requests += impl_->finish_requests(poller);
            if (impl_->stopping.load(std::memory_order_acquire)) {
                impl_->stop_poller(poller);
                result.stopped = true;
            }
            return result;
        }
    }

    if (poller_id == 0) {
        const int admin_result =
            spdk_nvme_ctrlr_process_admin_completions(impl_->controller);
        if (admin_result < 0) {
            result.error = error_from_negative_rc(admin_result);
            impl_->accepting.store(false, std::memory_order_release);
            impl_->stopping.store(true, std::memory_order_release);
            impl_->fail_poller(poller, result.error);
        }
    }

    if (!result.error &&
        impl_->process_completions(poller, max_completions, result.error) < 0) {
        impl_->fail_poller(poller, result.error);
    }

    if (!result.error) {
        for (auto &request : poller.active) {
            impl_->drive_request(poller, *request, result.error);
            if (result.error) {
                impl_->fail_poller(poller, result.error);
                break;
            }
        }
    }

    result.completed_requests += impl_->finish_requests(poller);

    if (impl_->stopping.load(std::memory_order_acquire) &&
        impl_->no_pending_work(poller)) {
        impl_->stop_poller(poller);
        result.stopped = true;
    }
    return result;
#endif
}

void SpdkNvmeBackend::request_stop() noexcept {
#ifdef ORCHFS_HAS_SPDK
    if (impl_ != nullptr) {
        impl_->accepting.store(false, std::memory_order_release);
        impl_->stopping.store(true, std::memory_order_release);
    }
#endif
}

bool SpdkNvmeBackend::stop_requested() const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ != nullptr && impl_->stopping.load(std::memory_order_acquire);
#else
    return false;
#endif
}

bool SpdkNvmeBackend::poller_stopped(std::size_t poller_id) const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ != nullptr && poller_id < impl_->pollers.size() &&
           impl_->pollers[poller_id]->stopped.load(std::memory_order_acquire);
#else
    (void)poller_id;
    return false;
#endif
}

std::error_code SpdkNvmeBackend::close() noexcept {
#ifndef ORCHFS_HAS_SPDK
    return std::make_error_code(std::errc::not_supported);
#else
    if (impl_ == nullptr || impl_->closed.load(std::memory_order_acquire)) {
        return {};
    }
    if (!impl_->stopping.load(std::memory_order_acquire)) {
        return make_error_code(BackendErrc::not_stopped);
    }
    for (const auto &poller : impl_->pollers) {
        if (!poller->stopped.load(std::memory_order_acquire)) {
            return make_error_code(BackendErrc::not_stopped);
        }
    }

    if (impl_->controller != nullptr) {
        const int result = spdk_nvme_detach(impl_->controller);
        if (result < 0) {
            return error_from_negative_rc(result);
        }
        impl_->controller = nullptr;
        impl_->namespace_handle = nullptr;
    }
    if (impl_->environment_started) {
        spdk_env_fini();
        impl_->environment_started = false;
    }
    impl_->closed.store(true, std::memory_order_release);
    {
        std::lock_guard lock(global_backend_mutex);
        global_backend_active = false;
    }
    return {};
#endif
}

std::size_t SpdkNvmeBackend::poller_count() const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ == nullptr ? 0 : impl_->pollers.size();
#else
    return 0;
#endif
}

std::uint32_t SpdkNvmeBackend::lba_size() const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ == nullptr ? 0 : impl_->lba;
#else
    return 0;
#endif
}

std::uint32_t SpdkNvmeBackend::max_transfer_size() const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ == nullptr ? 0 : impl_->max_transfer;
#else
    return 0;
#endif
}

std::uint64_t SpdkNvmeBackend::capacity_bytes() const noexcept {
#ifdef ORCHFS_HAS_SPDK
    return impl_ == nullptr ? 0 : impl_->capacity;
#else
    return 0;
#endif
}

} // namespace orchfs::nvme
