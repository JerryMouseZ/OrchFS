#pragma once

#include "orchfs/async/ipc_protocol.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
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

// ClientTransport is a process-local view of one KFS-owned session. Each lane
// is SPSC: exactly one LibFS worker submits and consumes completions on it.
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

    // buffer_too_small leaves the completion at the head of the ring and sets
    // payload_size to the required size. All other returned messages consume a
    // ring slot.
    [[nodiscard]] std::error_code try_receive_completion(
        std::uint32_t lane, IpcDescriptor& descriptor,
        std::span<std::byte> payload_buffer, std::size_t& payload_size) noexcept;

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
    struct Impl;
    explicit ClientTransport(std::unique_ptr<Impl> impl) noexcept;
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

    [[nodiscard]] int fd() const noexcept;
    [[nodiscard]] explicit operator bool() const noexcept;

  private:
    struct Impl;
    explicit ControlServer(std::unique_ptr<Impl> impl) noexcept;
    std::unique_ptr<Impl> impl_;
};

}  // namespace orchfs::async
