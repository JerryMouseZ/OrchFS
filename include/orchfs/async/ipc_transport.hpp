#pragma once

#include "orchfs/async/ipc_protocol.hpp"
#include "orchfs/async/result.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>

namespace orchfs::async {

struct TransportConfig {
    std::uint32_t lane_count{1};
    // At least two slots are required so the generation sequence can
    // distinguish a full ring from an empty one.
    std::uint32_t ring_capacity{256};
    std::uint32_t data_slot_size{64U * 1024U};

    [[nodiscard]] friend constexpr bool operator==(const TransportConfig&,
                                                   const TransportConfig&) = default;
};

struct SharedMemoryRegion {
    void* address{};
    std::size_t size{};
    bool hugepage_backed{};
};

// Move-only ownership of one consumed SQ/CQ slot. The slot is not returned to
// its producer until this object is destroyed, so payload bytes remain stable
// while an asynchronous filesystem or DMA operation is using them.
class ReceivedIpcSlot final {
  public:
    ReceivedIpcSlot() noexcept = default;
    ~ReceivedIpcSlot();
    ReceivedIpcSlot(ReceivedIpcSlot&& other) noexcept;
    ReceivedIpcSlot& operator=(ReceivedIpcSlot&& other) noexcept;

    ReceivedIpcSlot(const ReceivedIpcSlot&) = delete;
    ReceivedIpcSlot& operator=(const ReceivedIpcSlot&) = delete;

    [[nodiscard]] explicit operator bool() const noexcept;
    [[nodiscard]] const IpcDescriptor& descriptor() const noexcept;
    [[nodiscard]] std::span<const std::byte> payload() const noexcept;
    void reset() noexcept;

  private:
    ReceivedIpcSlot(SharedRingSlot* slot, std::uint64_t release_generation,
                    IpcDescriptor descriptor,
                    std::span<const std::byte> payload) noexcept;

    SharedRingSlot* slot_{};
    std::uint64_t release_generation_{};
    IpcDescriptor descriptor_{};
    std::span<const std::byte> payload_{};

    friend class ClientTransport;
    friend class ServerTransport;
};

// A server lane reserves its SPSC completion position before dispatch. The
// destination span can be passed directly to storage I/O. publish() makes the
// already-filled bytes visible to the client without another memcpy.
class CompletionReservation final {
  public:
    CompletionReservation() noexcept = default;
    ~CompletionReservation();
    CompletionReservation(CompletionReservation&& other) noexcept;
    CompletionReservation& operator=(CompletionReservation&& other) noexcept;

    CompletionReservation(const CompletionReservation&) = delete;
    CompletionReservation& operator=(const CompletionReservation&) = delete;

    [[nodiscard]] explicit operator bool() const noexcept;
    [[nodiscard]] std::span<std::byte> payload() const noexcept;
    [[nodiscard]] std::error_code publish(IpcDescriptor descriptor,
                                          std::size_t payload_size) noexcept;
    [[nodiscard]] std::error_code publish_copy(
        IpcDescriptor descriptor,
        std::span<const std::byte> payload) noexcept;
    void abandon() noexcept;

  private:
    CompletionReservation(SharedRingSlot* slot, std::byte* payload,
                          std::uint32_t payload_capacity,
                          std::uint64_t position,
                          std::uint32_t ring_capacity, int event_fd,
                          std::uint64_t client_id,
                          std::uint64_t session_generation) noexcept;

    SharedRingSlot* slot_{};
    std::byte* payload_{};
    std::uint32_t payload_capacity_{};
    std::uint64_t position_{};
    std::uint32_t ring_capacity_{};
    int event_fd_{-1};
    std::uint64_t client_id_{};
    std::uint64_t session_generation_{};

    friend class ServerTransport;
};

// ClientTransport is a process-local view of one KFS-owned session. Each lane
// is SPSC: exactly one LibFS worker publishes submissions and acquires
// completions. A completion-slot lease may then move to the blocking caller;
// releasing that lease only publishes the already-consumed slot generation.
// An inherited object is deliberately invalid after fork; the child must
// connect a new session. Destroying that inherited object does not affect the
// parent's session.
class ClientTransport {
  public:
    ClientTransport() noexcept;
    ~ClientTransport();
    ClientTransport(ClientTransport&&) noexcept;
    ClientTransport& operator=(ClientTransport&&) noexcept;

    ClientTransport(const ClientTransport&) = delete;
    ClientTransport& operator=(const ClientTransport&) = delete;

    [[nodiscard]] static ClientTransport connect(std::string_view socket_path,
                                                 TransportConfig requested,
                                                 std::error_code& error) noexcept;

    // A zero request_id is replaced with a process-wide session request id.
    // assigned_request_id is populated on success. would_block means that no
    // descriptor or payload was published.
    [[nodiscard]] std::error_code try_submit(
        std::uint32_t lane, IpcDescriptor descriptor,
        std::span<const std::byte> payload = {},
        std::uint64_t* assigned_request_id = nullptr) noexcept;

    // Publishes prefix and tail as one contiguous wire payload without first
    // materializing them in a temporary buffer. This is useful for write RPCs,
    // whose data can be copied directly from the caller into the shared slot.
    [[nodiscard]] std::error_code try_submit_scattered(
        std::uint32_t lane, IpcDescriptor descriptor,
        std::span<const std::byte> prefix, std::span<const std::byte> tail,
        std::uint64_t* assigned_request_id = nullptr) noexcept;

    // buffer_too_small leaves the completion at the head of the ring and sets
    // payload_size to the required size. All other returned messages consume a
    // ring slot.
    [[nodiscard]] std::error_code try_receive_completion(
        std::uint32_t lane, IpcDescriptor& descriptor,
        std::span<std::byte> payload_buffer, std::size_t& payload_size) noexcept;

    [[nodiscard]] std::error_code try_acquire_completion(
        std::uint32_t lane, ReceivedIpcSlot& completion) noexcept;

    [[nodiscard]] std::error_code drain_completion_notifications(
        std::uint32_t lane, std::uint64_t& notification_count) noexcept;

    [[nodiscard]] int completion_event_fd(std::uint32_t lane) const noexcept;
    [[nodiscard]] int control_fd() const noexcept;
    [[nodiscard]] bool peer_alive() const noexcept;
    void heartbeat() noexcept;

    [[nodiscard]] explicit operator bool() const noexcept;
    [[nodiscard]] std::uint64_t client_id() const noexcept;
    [[nodiscard]] std::uint64_t session_generation() const noexcept;
    [[nodiscard]] TransportConfig config() const noexcept;

  private:
    friend class ClientConnector;
    struct Impl;
    explicit ClientTransport(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

// Incremental non-blocking control-socket handshake. poll() never waits; a
// Runtime poller owns the object until it returns a populated transport or an
// error. The synchronous ClientTransport::connect helper is retained for
// offline/test callers and drives this same state machine itself.
class ClientConnector {
  public:
    ClientConnector() noexcept;
    ~ClientConnector();
    ClientConnector(ClientConnector&&) noexcept;
    ClientConnector& operator=(ClientConnector&&) noexcept;

    ClientConnector(const ClientConnector&) = delete;
    ClientConnector& operator=(const ClientConnector&) = delete;

    [[nodiscard]] static Result<ClientConnector> start(
        std::string_view socket_path, TransportConfig requested) noexcept;
    [[nodiscard]] Result<std::optional<ClientTransport>> poll() noexcept;
    [[nodiscard]] int fd() const noexcept;

  private:
    struct Impl;
    explicit ClientConnector(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

// ServerTransport is one accepted client session. KFS owns all authoritative
// filesystem state; this object only moves wire descriptors and bytes.
class ServerTransport {
  public:
    ServerTransport() noexcept;
    ~ServerTransport();
    ServerTransport(ServerTransport&&) noexcept;
    ServerTransport& operator=(ServerTransport&&) noexcept;

    ServerTransport(const ServerTransport&) = delete;
    ServerTransport& operator=(const ServerTransport&) = delete;

    [[nodiscard]] std::error_code try_receive_submission(
        std::uint32_t lane, IpcDescriptor& descriptor,
        std::span<std::byte> payload_buffer, std::size_t& payload_size) noexcept;

    [[nodiscard]] bool submission_ready(std::uint32_t lane) const noexcept;
    [[nodiscard]] std::error_code try_acquire_submission(
        std::uint32_t lane, ReceivedIpcSlot& submission) noexcept;
    [[nodiscard]] std::error_code try_reserve_completion(
        std::uint32_t lane, CompletionReservation& completion) noexcept;

    [[nodiscard]] std::error_code try_complete(
        std::uint32_t lane, IpcDescriptor descriptor,
        std::span<const std::byte> payload = {}) noexcept;

    [[nodiscard]] std::error_code drain_submission_notifications(
        std::uint32_t lane, std::uint64_t& notification_count) noexcept;

    [[nodiscard]] int submission_event_fd(std::uint32_t lane) const noexcept;
    [[nodiscard]] int control_fd() const noexcept;
    [[nodiscard]] bool peer_alive() const noexcept;
    void mark_dead() noexcept;

    [[nodiscard]] explicit operator bool() const noexcept;
    [[nodiscard]] std::uint64_t client_id() const noexcept;
    [[nodiscard]] std::uint64_t session_generation() const noexcept;
    [[nodiscard]] std::uint32_t client_pid() const noexcept;
    [[nodiscard]] TransportConfig config() const noexcept;
    [[nodiscard]] SharedMemoryRegion shared_memory_region() const noexcept;

  private:
    friend class ControlServer;
    struct Impl;
    explicit ServerTransport(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

// The listener configuration is an upper bound. A client chooses its exact
// lane count, ring capacity, and data slot size within those bounds. Like a
// transport session, a listener is invalid in a fork child and must be created
// again if that child becomes a server.
class ControlServer {
  public:
    ControlServer() noexcept;
    ~ControlServer();
    ControlServer(ControlServer&&) noexcept;
    ControlServer& operator=(ControlServer&&) noexcept;

    ControlServer(const ControlServer&) = delete;
    ControlServer& operator=(const ControlServer&) = delete;

    [[nodiscard]] static ControlServer listen(std::string_view socket_path,
                                              TransportConfig limits,
                                              std::error_code& error) noexcept;
    // cancellation_fd may be an eventfd/pipe owned by the caller. If it
    // becomes readable while accept or the bounded hello handshake is
    // waiting, accept returns operation_canceled.
    [[nodiscard]] ServerTransport accept(std::error_code& error,
                                         int cancellation_fd = -1) noexcept;

    // Advances the listener and any pending hello handshakes without blocking.
    // resource_unavailable_try_again means that no complete client handshake
    // is available yet. A stalled client is retained only for the bounded
    // handshake timeout and does not prevent later clients from connecting.
    [[nodiscard]] ServerTransport try_accept(std::error_code& error) noexcept;

    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] explicit operator bool() const noexcept;

  private:
    struct Impl;
    explicit ControlServer(std::unique_ptr<Impl> impl) noexcept;
    [[nodiscard]] ServerTransport accept_connected(
        int accepted_fd, std::error_code& error) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace orchfs::async
