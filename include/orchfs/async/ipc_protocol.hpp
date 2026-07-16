#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <system_error>
#include <type_traits>

namespace orchfs::async {

inline constexpr std::uint32_t kIpcMagic = 0x4f524348U;  // "ORCH"
inline constexpr std::uint32_t kIpcProtocolVersion = 1;
inline constexpr std::uint32_t kIpcCacheLineSize = 64;
// Linux limits one SCM_RIGHTS control message to 253 file descriptors. One
// memfd plus two eventfds per lane leaves room for at most 126 lanes; keep a
// small margin for future control descriptors.
inline constexpr std::uint32_t kMaxIpcWorkerLanes = 120;

// Opcodes describe filesystem RPCs only. The payload encoding for each opcode is
// intentionally kept out of the transport protocol.
enum class Opcode : std::uint16_t {
    invalid = 0,
    connect,
    ping,
    open,
    open_at,
    close,
    read,
    write,
    read_at,
    write_at,
    seek,
    stat_path,
    stat_handle,
    statfs,
    mkdir,
    rmdir,
    unlink,
    rename,
    truncate_path,
    truncate_handle,
    open_directory,
    read_directory_batch,
    close_directory,
    sync,
    set_flags,
    shutdown_session,
    raw_device_read,
    raw_device_write,
    raw_device_flush,
    open_directory_handle,
};

enum class DescriptorFlag : std::uint16_t {
    none = 0,
    request = 1U << 0U,
    response = 1U << 1U,
    has_payload = 1U << 2U,
    end_of_stream = 1U << 3U,
};

[[nodiscard]] constexpr DescriptorFlag operator|(DescriptorFlag lhs,
                                                  DescriptorFlag rhs) noexcept {
    return static_cast<DescriptorFlag>(static_cast<std::uint16_t>(lhs) |
                                       static_cast<std::uint16_t>(rhs));
}

[[nodiscard]] constexpr DescriptorFlag operator&(DescriptorFlag lhs,
                                                  DescriptorFlag rhs) noexcept {
    return static_cast<DescriptorFlag>(static_cast<std::uint16_t>(lhs) &
                                       static_cast<std::uint16_t>(rhs));
}

constexpr DescriptorFlag& operator|=(DescriptorFlag& lhs,
                                     DescriptorFlag rhs) noexcept {
    lhs = lhs | rhs;
    return lhs;
}

[[nodiscard]] constexpr bool has_flag(DescriptorFlag value,
                                      DescriptorFlag flag) noexcept {
    return (value & flag) != DescriptorFlag::none;
}

// This descriptor is the only per-request object crossing the process
// boundary. In particular, it must never contain a coroutine_handle, pointer,
// file descriptor, or other process-local token.
struct IpcDescriptor {
    std::uint64_t client_id{0};
    std::uint64_t session_generation{0};
    std::uint64_t request_id{0};
    std::uint64_t offset{0};
    std::uint64_t length{0};
    std::uint64_t result_length{0};
    std::uint64_t slot_generation{0};
    std::uint32_t protocol_version{kIpcProtocolVersion};
    std::uint32_t resume_worker{0};
    std::uint32_t data_slot{0};
    std::uint32_t payload_length{0};
    std::int32_t status{0};
    Opcode opcode{Opcode::invalid};
    DescriptorFlag flags{DescriptorFlag::none};
    std::uint32_t reserved{0};
};

static_assert(std::is_standard_layout_v<IpcDescriptor>);
static_assert(std::is_trivially_copyable_v<IpcDescriptor>);
static_assert(sizeof(IpcDescriptor) == 88);

enum class SessionState : std::uint32_t {
    initializing = 0,
    active = 1,
    closing = 2,
    dead = 3,
};

enum class TransportErrc {
    would_block = 1,
    protocol_mismatch,
    corrupt_layout,
    stale_session,
    buffer_too_small,
    peer_closed,
};

[[nodiscard]] std::error_code make_error_code(TransportErrc error) noexcept;

// The control and slot structures below form a process-shared ABI. Fields may
// only be appended in a new protocol version.
struct alignas(kIpcCacheLineSize) SharedRingCursor {
    std::atomic<std::uint64_t> value;
    std::byte padding[kIpcCacheLineSize - sizeof(std::atomic<std::uint64_t>)];
};

static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "shared IPC cursors require lock-free 64-bit atomics");
static_assert(std::atomic<std::uint32_t>::is_always_lock_free,
              "shared IPC state requires lock-free 32-bit atomics");
static_assert(sizeof(SharedRingCursor) == kIpcCacheLineSize);

struct alignas(kIpcCacheLineSize) SharedRingHeader {
    std::uint32_t magic{0};
    std::uint32_t protocol_version{0};
    std::uint32_t capacity{0};
    std::uint32_t slot_size{0};
    std::byte padding[kIpcCacheLineSize - 4U * sizeof(std::uint32_t)];
    SharedRingCursor producer;
    SharedRingCursor consumer;
};

static_assert(sizeof(SharedRingHeader) == 3U * kIpcCacheLineSize);

struct alignas(kIpcCacheLineSize) SharedRingSlot {
    // For absolute ring position P, a producer owns this slot when generation
    // is P and a consumer owns it when generation is P + 1. The consumer
    // releases it as P + capacity. This prevents ABA when indices wrap.
    std::atomic<std::uint64_t> generation;
    IpcDescriptor descriptor;
};

static_assert(sizeof(SharedRingSlot) == 2U * kIpcCacheLineSize);

struct alignas(kIpcCacheLineSize) SharedLaneHeader {
    std::uint32_t magic{0};
    std::uint32_t protocol_version{0};
    std::uint32_t lane_index{0};
    std::uint32_t capacity{0};
    std::uint32_t data_slot_size{0};
    std::uint32_t reserved{0};
    std::uint64_t submission_ring_offset{0};
    std::uint64_t completion_ring_offset{0};
    std::uint64_t submission_data_offset{0};
    std::uint64_t completion_data_offset{0};
    std::uint64_t lane_size{0};
};

static_assert(sizeof(SharedLaneHeader) == kIpcCacheLineSize);

struct alignas(kIpcCacheLineSize) SharedSessionHeader {
    std::uint32_t magic{0};
    std::uint32_t protocol_version{0};
    std::uint32_t header_size{0};
    std::uint32_t lane_count{0};
    std::uint32_t ring_capacity{0};
    std::uint32_t data_slot_size{0};
    std::uint32_t client_pid{0};
    std::uint32_t reserved0{0};
    std::uint64_t total_size{0};
    std::uint64_t client_id{0};
    std::uint64_t session_generation{0};
    std::uint64_t lane_stride{0};
    std::uint64_t lanes_offset{0};
    std::byte metadata_padding[kIpcCacheLineSize - sizeof(std::uint64_t)];

    alignas(kIpcCacheLineSize) std::atomic<std::uint32_t> state;
    std::byte state_padding[kIpcCacheLineSize - sizeof(std::atomic<std::uint32_t>)];

    alignas(kIpcCacheLineSize) std::atomic<std::uint64_t> heartbeat;
    std::byte heartbeat_padding[kIpcCacheLineSize - sizeof(std::atomic<std::uint64_t>)];
};

static_assert(sizeof(SharedSessionHeader) == 4U * kIpcCacheLineSize);

struct ClientHello {
    std::uint32_t magic{kIpcMagic};
    std::uint32_t protocol_version{kIpcProtocolVersion};
    std::uint32_t message_size{sizeof(ClientHello)};
    std::uint32_t lane_count{0};
    std::uint32_t ring_capacity{0};
    std::uint32_t data_slot_size{0};
    std::uint32_t client_pid{0};
    std::uint32_t reserved{0};
    std::uint64_t client_nonce{0};
};

struct ServerHello {
    std::uint32_t magic{kIpcMagic};
    std::uint32_t protocol_version{kIpcProtocolVersion};
    std::uint32_t message_size{sizeof(ServerHello)};
    std::int32_t status{0};
    std::uint32_t lane_count{0};
    std::uint32_t ring_capacity{0};
    std::uint32_t data_slot_size{0};
    std::uint32_t fd_count{0};
    std::uint64_t shared_memory_size{0};
    std::uint64_t client_id{0};
    std::uint64_t session_generation{0};
};

static_assert(std::is_trivially_copyable_v<ClientHello>);
static_assert(std::is_trivially_copyable_v<ServerHello>);

}  // namespace orchfs::async

namespace std {
template <>
struct is_error_code_enum<orchfs::async::TransportErrc> : true_type {};
}  // namespace std
