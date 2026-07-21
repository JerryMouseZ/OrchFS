#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "orchfs/async/ipc_transport.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <poll.h>
#include <pthread.h>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <linux/memfd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

namespace orchfs::async {
namespace {

constexpr std::uint32_t kMaxRingCapacity = 1U << 20U;
constexpr std::uint32_t kMaxDataSlotSize = 64U * 1024U * 1024U;
constexpr std::uint64_t kMaxSharedMemorySize = 64ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr std::uint64_t kIpcHugePageSize = 2ULL * 1024ULL * 1024ULL;
constexpr int kControlHandshakeTimeoutMs = 250;

std::atomic<std::uint64_t> process_epoch{1};

void advance_process_epoch_after_fork() noexcept {
    process_epoch.fetch_add(1, std::memory_order_relaxed);
}

const bool process_epoch_tracking_available =
    ::pthread_atfork(nullptr, nullptr, advance_process_epoch_after_fork) == 0;

[[nodiscard]] bool process_owner_matches(std::uint64_t owner_epoch,
                                         pid_t owner_pid) noexcept {
    return process_epoch_tracking_available
               ? owner_epoch == process_epoch.load(std::memory_order_relaxed)
               : owner_pid == ::getpid();
}

class TransportErrorCategory final : public std::error_category {
  public:
    [[nodiscard]] const char* name() const noexcept override {
        return "orchfs.ipc";
    }

    [[nodiscard]] std::string message(int condition) const override {
        switch (static_cast<TransportErrc>(condition)) {
            case TransportErrc::would_block:
                return "IPC ring would block";
            case TransportErrc::protocol_mismatch:
                return "IPC protocol version mismatch";
            case TransportErrc::corrupt_layout:
                return "corrupt IPC shared-memory layout";
            case TransportErrc::stale_session:
                return "stale IPC session generation";
            case TransportErrc::buffer_too_small:
                return "IPC payload buffer is too small";
            case TransportErrc::peer_closed:
                return "IPC peer closed the control connection";
        }
        return "unknown OrchFS IPC transport error";
    }
};

const TransportErrorCategory kTransportErrorCategory;

[[nodiscard]] std::error_code errno_error() noexcept {
    return {errno, std::generic_category()};
}

[[nodiscard]] constexpr std::uint64_t align_up(std::uint64_t value,
                                               std::uint64_t alignment) noexcept {
    return (value + alignment - 1U) & ~(alignment - 1U);
}

[[nodiscard]] bool checked_add(std::uint64_t lhs, std::uint64_t rhs,
                               std::uint64_t& result) noexcept {
    if (lhs > std::numeric_limits<std::uint64_t>::max() - rhs) {
        return false;
    }
    result = lhs + rhs;
    return true;
}

[[nodiscard]] bool checked_multiply(std::uint64_t lhs, std::uint64_t rhs,
                                    std::uint64_t& result) noexcept {
    if (lhs != 0 && rhs > std::numeric_limits<std::uint64_t>::max() / lhs) {
        return false;
    }
    result = lhs * rhs;
    return true;
}

[[nodiscard]] bool valid_config(const TransportConfig& config) noexcept {
    return config.lane_count != 0 && config.lane_count <= kMaxIpcWorkerLanes &&
           config.ring_capacity >= 2 && config.ring_capacity <= kMaxRingCapacity &&
           config.data_slot_size != 0 && config.data_slot_size <= kMaxDataSlotSize;
}

struct SessionLayout {
    std::uint64_t total_size{0};
    std::uint64_t lanes_offset{0};
    std::uint64_t lane_stride{0};
    std::uint64_t submission_ring_offset{0};
    std::uint64_t completion_ring_offset{0};
    std::uint64_t submission_data_offset{0};
    std::uint64_t completion_data_offset{0};
    std::uint64_t ring_size{0};
    std::uint64_t data_region_size{0};
};

[[nodiscard]] std::error_code make_layout(const TransportConfig& config,
                                          SessionLayout& layout) noexcept {
    if (!valid_config(config)) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    std::uint64_t slots_size = 0;
    if (!checked_multiply(config.ring_capacity, sizeof(SharedRingSlot), slots_size)) {
        return std::make_error_code(std::errc::value_too_large);
    }
    std::uint64_t raw_ring_size = 0;
    if (!checked_add(sizeof(SharedRingHeader), slots_size, raw_ring_size)) {
        return std::make_error_code(std::errc::value_too_large);
    }
    layout.ring_size = align_up(raw_ring_size, kIpcCacheLineSize);

    const std::uint64_t data_slot_stride =
        align_up(config.data_slot_size, kIpcCacheLineSize);
    if (!checked_multiply(config.ring_capacity, data_slot_stride,
                          layout.data_region_size)) {
        return std::make_error_code(std::errc::value_too_large);
    }

    layout.submission_ring_offset = sizeof(SharedLaneHeader);
    layout.completion_ring_offset =
        layout.submission_ring_offset + layout.ring_size;
    layout.submission_data_offset =
        layout.completion_ring_offset + layout.ring_size;
    layout.completion_data_offset =
        layout.submission_data_offset + layout.data_region_size;
    layout.lane_stride = align_up(layout.completion_data_offset +
                                      layout.data_region_size,
                                  kIpcCacheLineSize);
    layout.lanes_offset = align_up(sizeof(SharedSessionHeader), kIpcCacheLineSize);

    std::uint64_t all_lanes_size = 0;
    std::uint64_t unaligned_total = 0;
    if (!checked_multiply(config.lane_count, layout.lane_stride, all_lanes_size) ||
        !checked_add(layout.lanes_offset, all_lanes_size, unaligned_total) ||
        unaligned_total >
            std::numeric_limits<std::uint64_t>::max() - kIpcHugePageSize + 1U ||
        (layout.total_size = align_up(unaligned_total, kIpcHugePageSize)) == 0 ||
        layout.total_size > kMaxSharedMemorySize ||
        layout.total_size > std::numeric_limits<std::size_t>::max()) {
        return std::make_error_code(std::errc::value_too_large);
    }
    return {};
}

struct UniqueFd {
    int value{-1};

    UniqueFd() noexcept = default;
    explicit UniqueFd(int fd) noexcept : value(fd) {}
    ~UniqueFd() {
        if (value >= 0) {
            ::close(value);
        }
    }
    UniqueFd(UniqueFd&& other) noexcept : value(std::exchange(other.value, -1)) {}
    UniqueFd& operator=(UniqueFd&& other) noexcept {
        if (this != &other) {
            if (value >= 0) {
                ::close(value);
            }
            value = std::exchange(other.value, -1);
        }
        return *this;
    }
    UniqueFd(const UniqueFd&) = delete;
    UniqueFd& operator=(const UniqueFd&) = delete;

    [[nodiscard]] int get() const noexcept { return value; }
    [[nodiscard]] int release() noexcept { return std::exchange(value, -1); }
    [[nodiscard]] explicit operator bool() const noexcept { return value >= 0; }
};

struct Mapping {
    void* address{MAP_FAILED};
    std::size_t size{0};

    Mapping() noexcept = default;
    Mapping(void* mapped_address, std::size_t mapped_size) noexcept
        : address(mapped_address), size(mapped_size) {}
    ~Mapping() {
        if (address != MAP_FAILED) {
            ::munmap(address, size);
        }
    }
    Mapping(Mapping&& other) noexcept
        : address(std::exchange(other.address, MAP_FAILED)),
          size(std::exchange(other.size, 0)) {}
    Mapping& operator=(Mapping&& other) noexcept {
        if (this != &other) {
            if (address != MAP_FAILED) {
                ::munmap(address, size);
            }
            address = std::exchange(other.address, MAP_FAILED);
            size = std::exchange(other.size, 0);
        }
        return *this;
    }
    Mapping(const Mapping&) = delete;
    Mapping& operator=(const Mapping&) = delete;
};

struct AcquiredSlotData {
    SharedRingSlot* slot{};
    std::uint64_t release_generation{};
    IpcDescriptor descriptor{};
    std::span<const std::byte> payload{};
};

struct ReservedSlotData {
    SharedRingSlot* slot{};
    std::byte* payload{};
    std::uint64_t position{};
};

class RingView {
  public:
    RingView() noexcept = default;
    explicit RingView(SharedRingHeader* header) noexcept
        : header_(header), slots_(reinterpret_cast<SharedRingSlot*>(header + 1)) {}

    [[nodiscard]] bool ready() const noexcept {
        if (header_ == nullptr) {
            return false;
        }
        const std::uint64_t position =
            header_->consumer.value.load(std::memory_order_relaxed);
        const auto slot_index = static_cast<std::uint32_t>(
            position % header_->capacity);
        return slots_[slot_index].generation.load(std::memory_order_acquire) ==
               position + 1U;
    }

    [[nodiscard]] std::error_code try_push(IpcDescriptor descriptor,
                                           std::span<const std::byte> payload,
                                           std::span<const std::byte> tail,
                                           std::byte* data,
                                           std::uint32_t data_slot_size) noexcept {
        const std::uint64_t position =
            header_->producer.value.load(std::memory_order_relaxed);
        const std::uint32_t slot_index =
            static_cast<std::uint32_t>(position % header_->capacity);
        SharedRingSlot& slot = slots_[slot_index];
        if (slot.generation.load(std::memory_order_acquire) != position) {
            return TransportErrc::would_block;
        }

        if (payload.size() > data_slot_size ||
            tail.size() > data_slot_size - payload.size()) {
            return std::make_error_code(std::errc::message_size);
        }
        const std::size_t payload_size = payload.size() + tail.size();
        const std::uint64_t data_stride = align_up(data_slot_size, kIpcCacheLineSize);
        std::byte* destination = data + data_stride * slot_index;
        if (payload_size != 0) {
            if (!payload.empty()) {
                std::memcpy(destination, payload.data(), payload.size());
            }
            if (!tail.empty()) {
                std::memcpy(destination + payload.size(), tail.data(), tail.size());
            }
            descriptor.flags |= DescriptorFlag::has_payload;
        } else {
            descriptor.flags = static_cast<DescriptorFlag>(
                static_cast<std::uint16_t>(descriptor.flags) &
                ~static_cast<std::uint16_t>(DescriptorFlag::has_payload));
        }
        descriptor.data_slot = slot_index;
        descriptor.payload_length = static_cast<std::uint32_t>(payload_size);
        descriptor.slot_generation = position;
        slot.descriptor = descriptor;
        slot.generation.store(position + 1U, std::memory_order_release);
        header_->producer.value.store(position + 1U, std::memory_order_release);
        return {};
    }

    [[nodiscard]] std::error_code try_pop(
        IpcDescriptor& descriptor, std::span<std::byte> payload_buffer,
        std::size_t& payload_size, std::byte* data, std::uint32_t data_slot_size,
        std::uint64_t expected_client_id,
        std::uint64_t expected_session_generation) noexcept {
        const std::uint64_t position =
            header_->consumer.value.load(std::memory_order_relaxed);
        const std::uint32_t slot_index =
            static_cast<std::uint32_t>(position % header_->capacity);
        SharedRingSlot& slot = slots_[slot_index];
        if (slot.generation.load(std::memory_order_acquire) != position + 1U) {
            payload_size = 0;
            return TransportErrc::would_block;
        }

        const IpcDescriptor incoming = slot.descriptor;
        payload_size = incoming.payload_length;
        const bool bad_layout =
            incoming.protocol_version != kIpcProtocolVersion ||
            incoming.data_slot != slot_index ||
            incoming.slot_generation != position ||
            incoming.payload_length > data_slot_size;
        const bool stale_session =
            incoming.client_id != expected_client_id ||
            incoming.session_generation != expected_session_generation;

        if (!bad_layout && !stale_session && payload_buffer.size() < payload_size) {
            return TransportErrc::buffer_too_small;
        }

        descriptor = incoming;
        if (!bad_layout && !stale_session && payload_size != 0) {
            const std::uint64_t data_stride =
                align_up(data_slot_size, kIpcCacheLineSize);
            std::memcpy(payload_buffer.data(), data + data_stride * slot_index,
                        payload_size);
        }

        slot.generation.store(position + header_->capacity,
                              std::memory_order_release);
        header_->consumer.value.store(position + 1U, std::memory_order_release);

        if (bad_layout) {
            return TransportErrc::corrupt_layout;
        }
        if (stale_session) {
            return TransportErrc::stale_session;
        }
        return {};
    }

    [[nodiscard]] std::error_code try_acquire(
        AcquiredSlotData& acquired, std::byte* data,
        std::uint32_t data_slot_size, std::uint64_t expected_client_id,
        std::uint64_t expected_session_generation) noexcept {
        acquired = {};
        const std::uint64_t position =
            header_->consumer.value.load(std::memory_order_relaxed);
        const auto slot_index = static_cast<std::uint32_t>(
            position % header_->capacity);
        SharedRingSlot& slot = slots_[slot_index];
        if (slot.generation.load(std::memory_order_acquire) != position + 1U) {
            return TransportErrc::would_block;
        }

        const IpcDescriptor incoming = slot.descriptor;
        const bool bad_layout =
            incoming.protocol_version != kIpcProtocolVersion ||
            incoming.data_slot != slot_index ||
            incoming.slot_generation != position ||
            incoming.payload_length > data_slot_size;
        const bool stale_session =
            incoming.client_id != expected_client_id ||
            incoming.session_generation != expected_session_generation;
        header_->consumer.value.store(position + 1U, std::memory_order_release);
        if (bad_layout || stale_session) {
            slot.generation.store(position + header_->capacity,
                                  std::memory_order_release);
            return bad_layout ? make_error_code(TransportErrc::corrupt_layout)
                              : make_error_code(TransportErrc::stale_session);
        }

        const std::uint64_t data_stride =
            align_up(data_slot_size, kIpcCacheLineSize);
        acquired = AcquiredSlotData{
            .slot = &slot,
            .release_generation = position + header_->capacity,
            .descriptor = incoming,
            .payload = std::span<const std::byte>(
                data + data_stride * slot_index, incoming.payload_length),
        };
        return {};
    }

    [[nodiscard]] std::error_code try_reserve(
        ReservedSlotData& reserved, std::byte* data,
        std::uint32_t data_slot_size) noexcept {
        reserved = {};
        const std::uint64_t position =
            header_->producer.value.load(std::memory_order_relaxed);
        const auto slot_index = static_cast<std::uint32_t>(
            position % header_->capacity);
        SharedRingSlot& slot = slots_[slot_index];
        if (slot.generation.load(std::memory_order_acquire) != position) {
            return TransportErrc::would_block;
        }
        const std::uint64_t data_stride =
            align_up(data_slot_size, kIpcCacheLineSize);
        header_->producer.value.store(position + 1U,
                                      std::memory_order_release);
        reserved = ReservedSlotData{
            .slot = &slot,
            .payload = data + data_stride * slot_index,
            .position = position,
        };
        return {};
    }

  private:
    SharedRingHeader* header_{nullptr};
    SharedRingSlot* slots_{nullptr};
};

struct LaneView {
    RingView submissions;
    RingView completions;
    std::byte* submission_data{nullptr};
    std::byte* completion_data{nullptr};
};

void initialize_ring(void* memory, std::uint32_t capacity) noexcept {
    auto* header = ::new (memory) SharedRingHeader{};
    header->magic = kIpcMagic;
    header->protocol_version = kIpcProtocolVersion;
    header->capacity = capacity;
    header->slot_size = sizeof(SharedRingSlot);
    header->producer.value.store(0, std::memory_order_relaxed);
    header->consumer.value.store(0, std::memory_order_relaxed);

    auto* slots = reinterpret_cast<SharedRingSlot*>(header + 1);
    for (std::uint32_t index = 0; index < capacity; ++index) {
        auto* slot = ::new (slots + index) SharedRingSlot{};
        slot->generation.store(index, std::memory_order_relaxed);
    }
}

[[nodiscard]] std::error_code initialize_mapping(
    void* memory, const SessionLayout& layout, const TransportConfig& config,
    std::uint64_t client_id, std::uint64_t session_generation,
    std::uint32_t client_pid) noexcept {
    std::memset(memory, 0, static_cast<std::size_t>(layout.total_size));
    auto* session = ::new (memory) SharedSessionHeader{};
    session->magic = kIpcMagic;
    session->protocol_version = kIpcProtocolVersion;
    session->header_size = sizeof(SharedSessionHeader);
    session->lane_count = config.lane_count;
    session->ring_capacity = config.ring_capacity;
    session->data_slot_size = config.data_slot_size;
    session->client_pid = client_pid;
    session->total_size = layout.total_size;
    session->client_id = client_id;
    session->session_generation = session_generation;
    session->lane_stride = layout.lane_stride;
    session->lanes_offset = layout.lanes_offset;
    session->state.store(static_cast<std::uint32_t>(SessionState::initializing),
                         std::memory_order_relaxed);
    session->heartbeat.store(0, std::memory_order_relaxed);

    auto* bytes = static_cast<std::byte*>(memory);
    for (std::uint32_t index = 0; index < config.lane_count; ++index) {
        std::byte* lane_base =
            bytes + layout.lanes_offset + layout.lane_stride * index;
        auto* lane = ::new (lane_base) SharedLaneHeader{};
        lane->magic = kIpcMagic;
        lane->protocol_version = kIpcProtocolVersion;
        lane->lane_index = index;
        lane->capacity = config.ring_capacity;
        lane->data_slot_size = config.data_slot_size;
        lane->submission_ring_offset = layout.submission_ring_offset;
        lane->completion_ring_offset = layout.completion_ring_offset;
        lane->submission_data_offset = layout.submission_data_offset;
        lane->completion_data_offset = layout.completion_data_offset;
        lane->lane_size = layout.lane_stride;
        initialize_ring(lane_base + layout.submission_ring_offset,
                        config.ring_capacity);
        initialize_ring(lane_base + layout.completion_ring_offset,
                        config.ring_capacity);
    }
    return {};
}

[[nodiscard]] bool region_inside(std::uint64_t offset, std::uint64_t length,
                                 std::uint64_t container_size) noexcept {
    return offset <= container_size && length <= container_size - offset;
}

[[nodiscard]] std::error_code build_views(
    Mapping& mapping, const TransportConfig& config, std::uint64_t client_id,
    std::uint64_t session_generation, std::vector<LaneView>& lanes,
    SharedSessionHeader*& session) noexcept {
    if (mapping.address == MAP_FAILED || mapping.size < sizeof(SharedSessionHeader)) {
        return TransportErrc::corrupt_layout;
    }
    SessionLayout expected_layout{};
    if (make_layout(config, expected_layout)) {
        return TransportErrc::corrupt_layout;
    }
    session = static_cast<SharedSessionHeader*>(mapping.address);
    if (session->magic != kIpcMagic ||
        session->protocol_version != kIpcProtocolVersion ||
        session->header_size != sizeof(SharedSessionHeader) ||
        session->lane_count != config.lane_count ||
        session->ring_capacity != config.ring_capacity ||
        session->data_slot_size != config.data_slot_size ||
        session->total_size != mapping.size ||
        session->total_size != expected_layout.total_size ||
        session->lanes_offset != expected_layout.lanes_offset ||
        session->lane_stride != expected_layout.lane_stride ||
        session->client_id != client_id ||
        session->session_generation != session_generation ||
        !region_inside(session->lanes_offset,
                       session->lane_stride * session->lane_count,
                       session->total_size)) {
        return TransportErrc::corrupt_layout;
    }

    const std::uint64_t ring_bytes =
        align_up(sizeof(SharedRingHeader) +
                     static_cast<std::uint64_t>(config.ring_capacity) *
                         sizeof(SharedRingSlot),
                 kIpcCacheLineSize);
    const std::uint64_t data_bytes =
        align_up(config.data_slot_size, kIpcCacheLineSize) *
        config.ring_capacity;
    auto* bytes = static_cast<std::byte*>(mapping.address);
    lanes.clear();
    lanes.reserve(config.lane_count);
    for (std::uint32_t index = 0; index < config.lane_count; ++index) {
        std::byte* lane_base =
            bytes + session->lanes_offset + session->lane_stride * index;
        auto* lane = reinterpret_cast<SharedLaneHeader*>(lane_base);
        if (lane->magic != kIpcMagic ||
            lane->protocol_version != kIpcProtocolVersion ||
            lane->lane_index != index || lane->capacity != config.ring_capacity ||
            lane->data_slot_size != config.data_slot_size ||
            lane->lane_size != session->lane_stride ||
            lane->submission_ring_offset !=
                expected_layout.submission_ring_offset ||
            lane->completion_ring_offset !=
                expected_layout.completion_ring_offset ||
            lane->submission_data_offset !=
                expected_layout.submission_data_offset ||
            lane->completion_data_offset !=
                expected_layout.completion_data_offset ||
            !region_inside(lane->submission_ring_offset, ring_bytes,
                           lane->lane_size) ||
            !region_inside(lane->completion_ring_offset, ring_bytes,
                           lane->lane_size) ||
            !region_inside(lane->submission_data_offset, data_bytes,
                           lane->lane_size) ||
            !region_inside(lane->completion_data_offset, data_bytes,
                           lane->lane_size)) {
            return TransportErrc::corrupt_layout;
        }
        auto* submission = reinterpret_cast<SharedRingHeader*>(
            lane_base + lane->submission_ring_offset);
        auto* completion = reinterpret_cast<SharedRingHeader*>(
            lane_base + lane->completion_ring_offset);
        const auto valid_ring = [&](SharedRingHeader* ring) {
            return ring->magic == kIpcMagic &&
                   ring->protocol_version == kIpcProtocolVersion &&
                   ring->capacity == config.ring_capacity &&
                   ring->slot_size == sizeof(SharedRingSlot);
        };
        if (!valid_ring(submission) || !valid_ring(completion)) {
            return TransportErrc::corrupt_layout;
        }
        lanes.push_back({RingView(submission), RingView(completion),
                         lane_base + lane->submission_data_offset,
                         lane_base + lane->completion_data_offset});
    }
    return {};
}

[[nodiscard]] std::error_code send_packet(int socket_fd, const void* packet,
                                          std::size_t packet_size,
                                          std::span<const int> fds = {}) noexcept {
    iovec io{const_cast<void*>(packet), packet_size};
    msghdr message{};
    message.msg_iov = &io;
    message.msg_iovlen = 1;

    std::vector<std::max_align_t> control;
    if (!fds.empty()) {
        const std::size_t control_size = CMSG_SPACE(fds.size_bytes());
        control.resize((control_size + sizeof(std::max_align_t) - 1U) /
                       sizeof(std::max_align_t));
        message.msg_control = control.data();
        message.msg_controllen = control_size;
        cmsghdr* header = CMSG_FIRSTHDR(&message);
        header->cmsg_level = SOL_SOCKET;
        header->cmsg_type = SCM_RIGHTS;
        header->cmsg_len = CMSG_LEN(fds.size_bytes());
        std::memcpy(CMSG_DATA(header), fds.data(), fds.size_bytes());
    }

    ssize_t sent = -1;
    do {
        sent = ::sendmsg(socket_fd, &message, MSG_NOSIGNAL);
    } while (sent < 0 && errno == EINTR);
    if (sent < 0) {
        return errno_error();
    }
    if (static_cast<std::size_t>(sent) != packet_size) {
        return std::make_error_code(std::errc::io_error);
    }
    return {};
}

[[nodiscard]] std::error_code receive_packet(int socket_fd, void* packet,
                                             std::size_t packet_size,
                                             std::vector<UniqueFd>& fds) noexcept {
    iovec io{packet, packet_size};
    msghdr message{};
    message.msg_iov = &io;
    message.msg_iovlen = 1;
    const std::size_t max_fd_count = 1U + 2U * kMaxIpcWorkerLanes;
    const std::size_t control_size = CMSG_SPACE(max_fd_count * sizeof(int));
    std::vector<std::max_align_t> control(
        (control_size + sizeof(std::max_align_t) - 1U) /
        sizeof(std::max_align_t));
    message.msg_control = control.data();
    message.msg_controllen = control_size;

    ssize_t received = -1;
    do {
        received = ::recvmsg(socket_fd, &message, MSG_CMSG_CLOEXEC);
    } while (received < 0 && errno == EINTR);
    if (received == 0) {
        return TransportErrc::peer_closed;
    }
    if (received < 0) {
        return errno_error();
    }
    if (static_cast<std::size_t>(received) != packet_size ||
        (message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) != 0) {
        return TransportErrc::protocol_mismatch;
    }

    for (cmsghdr* header = CMSG_FIRSTHDR(&message); header != nullptr;
         header = CMSG_NXTHDR(&message, header)) {
        if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS ||
            header->cmsg_len < CMSG_LEN(0)) {
            continue;
        }
        const std::size_t bytes = header->cmsg_len - CMSG_LEN(0);
        if (bytes % sizeof(int) != 0) {
            return TransportErrc::protocol_mismatch;
        }
        const auto* received_fds = reinterpret_cast<const int*>(CMSG_DATA(header));
        for (std::size_t index = 0; index < bytes / sizeof(int); ++index) {
            fds.emplace_back(received_fds[index]);
        }
    }
    return {};
}

[[nodiscard]] std::error_code signal_event(int fd) noexcept {
    constexpr std::uint64_t one = 1;
    for (;;) {
        if (::write(fd, &one, sizeof(one)) == static_cast<ssize_t>(sizeof(one))) {
            return {};
        }
        if (errno == EINTR) {
            continue;
        }
        // The descriptor has already been published. A saturated eventfd still
        // represents pending work and must not make the caller retry the RPC.
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return {};
        }
        return errno_error();
    }
}

[[nodiscard]] std::error_code drain_event(int fd, std::uint64_t& count) noexcept {
    count = 0;
    for (;;) {
        std::uint64_t value = 0;
        const ssize_t result = ::read(fd, &value, sizeof(value));
        if (result == static_cast<ssize_t>(sizeof(value))) {
            count += value;
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return {};
        }
        if (result == 0) {
            return TransportErrc::peer_closed;
        }
        return result < 0 ? errno_error()
                          : std::make_error_code(std::errc::io_error);
    }
}

[[nodiscard]] bool socket_peer_alive(int fd) noexcept {
    if (fd < 0) {
        return false;
    }
    pollfd descriptor{};
    descriptor.fd = fd;
    descriptor.events = POLLIN;
#ifdef POLLRDHUP
    descriptor.events |= POLLRDHUP;
#endif
    int result = -1;
    do {
        result = ::poll(&descriptor, 1, 0);
    } while (result < 0 && errno == EINTR);
    if (result < 0) {
        return false;
    }
    if (result == 0) {
        return true;
    }
    short closed_events = POLLHUP | POLLERR | POLLNVAL;
#ifdef POLLRDHUP
    closed_events |= POLLRDHUP;
#endif
    if ((descriptor.revents & closed_events) != 0) {
        return false;
    }
    if ((descriptor.revents & POLLIN) != 0) {
        std::byte byte{};
        const ssize_t peeked = ::recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT);
        return peeked > 0 || (peeked < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
    }
    return true;
}

[[nodiscard]] std::uint64_t new_generation() noexcept {
    static std::atomic<std::uint64_t> counter{1};
    const auto now = static_cast<std::uint64_t>(
        std::chrono::steady_clock::now().time_since_epoch().count());
    return (now << 16U) ^ (static_cast<std::uint64_t>(::getpid()) << 32U) ^
           counter.fetch_add(1, std::memory_order_relaxed);
}

[[nodiscard]] int create_memfd(bool& hugepage_backed) noexcept {
    hugepage_backed = false;
    const char* configured = ::getenv("ORCHFS_IPC_HUGEPAGES");
    const bool request_hugepages = configured != nullptr &&
        (std::strcmp(configured, "1") == 0 ||
         std::strcmp(configured, "true") == 0);
    if (request_hugepages) {
        const int huge_fd = static_cast<int>(::syscall(
            SYS_memfd_create, "orchfs-ipc-huge",
            MFD_CLOEXEC | MFD_ALLOW_SEALING | MFD_HUGETLB | MFD_HUGE_2MB));
        if (huge_fd >= 0) {
            hugepage_backed = true;
            return huge_fd;
        }
    }
    return static_cast<int>(::syscall(
        SYS_memfd_create, "orchfs-ipc", MFD_CLOEXEC | MFD_ALLOW_SEALING));
}

[[nodiscard]] std::error_code make_unix_address(std::string_view path,
                                                sockaddr_un& address,
                                                socklen_t& length) noexcept {
    if (path.empty() || path.size() >= sizeof(address.sun_path) ||
        path.find('\0') != std::string_view::npos) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    address = {};
    address.sun_family = AF_UNIX;
    std::memcpy(address.sun_path, path.data(), path.size());
    address.sun_path[path.size()] = '\0';
    length = static_cast<socklen_t>(offsetof(sockaddr_un, sun_path) + path.size() + 1U);
    return {};
}

struct SessionResources {
    UniqueFd control;
    UniqueFd memory_fd;
    Mapping mapping;
    std::vector<UniqueFd> submission_events;
    std::vector<UniqueFd> completion_events;
    std::vector<LaneView> lanes;
    SharedSessionHeader* header{nullptr};
    TransportConfig config{};
    bool hugepage_backed{};
};

}  // namespace

ReceivedIpcSlot::ReceivedIpcSlot(
    SharedRingSlot* slot, std::uint64_t release_generation,
    IpcDescriptor descriptor, std::span<const std::byte> payload) noexcept
    : slot_(slot), release_generation_(release_generation),
      descriptor_(descriptor), payload_(payload) {}

ReceivedIpcSlot::~ReceivedIpcSlot() { reset(); }

ReceivedIpcSlot::ReceivedIpcSlot(ReceivedIpcSlot&& other) noexcept
    : slot_(std::exchange(other.slot_, nullptr)),
      release_generation_(other.release_generation_),
      descriptor_(other.descriptor_), payload_(other.payload_) {
    other.payload_ = {};
}

ReceivedIpcSlot& ReceivedIpcSlot::operator=(
    ReceivedIpcSlot&& other) noexcept {
    if (this != &other) {
        reset();
        slot_ = std::exchange(other.slot_, nullptr);
        release_generation_ = other.release_generation_;
        descriptor_ = other.descriptor_;
        payload_ = other.payload_;
        other.payload_ = {};
    }
    return *this;
}

ReceivedIpcSlot::operator bool() const noexcept { return slot_ != nullptr; }

const IpcDescriptor& ReceivedIpcSlot::descriptor() const noexcept {
    return descriptor_;
}

std::span<const std::byte> ReceivedIpcSlot::payload() const noexcept {
    return payload_;
}

void ReceivedIpcSlot::reset() noexcept {
    auto* slot = std::exchange(slot_, nullptr);
    payload_ = {};
    if (slot != nullptr) {
        slot->generation.store(release_generation_, std::memory_order_release);
    }
}

CompletionReservation::CompletionReservation(
    SharedRingSlot* slot, std::byte* payload,
    std::uint32_t payload_capacity, std::uint64_t position,
    std::uint32_t ring_capacity, int event_fd, std::uint64_t client_id,
    std::uint64_t session_generation) noexcept
    : slot_(slot), payload_(payload), payload_capacity_(payload_capacity),
      position_(position), ring_capacity_(ring_capacity), event_fd_(event_fd),
      client_id_(client_id), session_generation_(session_generation) {}

CompletionReservation::~CompletionReservation() { abandon(); }

CompletionReservation::CompletionReservation(
    CompletionReservation&& other) noexcept
    : slot_(std::exchange(other.slot_, nullptr)), payload_(other.payload_),
      payload_capacity_(other.payload_capacity_), position_(other.position_),
      ring_capacity_(other.ring_capacity_), event_fd_(other.event_fd_),
      client_id_(other.client_id_),
      session_generation_(other.session_generation_) {
    other.payload_ = nullptr;
}

CompletionReservation& CompletionReservation::operator=(
    CompletionReservation&& other) noexcept {
    if (this != &other) {
        abandon();
        slot_ = std::exchange(other.slot_, nullptr);
        payload_ = other.payload_;
        payload_capacity_ = other.payload_capacity_;
        position_ = other.position_;
        ring_capacity_ = other.ring_capacity_;
        event_fd_ = other.event_fd_;
        client_id_ = other.client_id_;
        session_generation_ = other.session_generation_;
        other.payload_ = nullptr;
    }
    return *this;
}

CompletionReservation::operator bool() const noexcept {
    return slot_ != nullptr;
}

std::span<std::byte> CompletionReservation::payload() const noexcept {
    return slot_ == nullptr
               ? std::span<std::byte>{}
               : std::span<std::byte>(payload_, payload_capacity_);
}

std::error_code CompletionReservation::publish(
    IpcDescriptor descriptor, std::size_t payload_size) noexcept {
    if (slot_ == nullptr || descriptor.request_id == 0 ||
        payload_size > payload_capacity_) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    descriptor.client_id = client_id_;
    descriptor.session_generation = session_generation_;
    descriptor.protocol_version = kIpcProtocolVersion;
    descriptor.flags = static_cast<DescriptorFlag>(
        (static_cast<std::uint16_t>(descriptor.flags) |
         static_cast<std::uint16_t>(DescriptorFlag::response)) &
        ~static_cast<std::uint16_t>(DescriptorFlag::request));
    if (payload_size != 0) {
        descriptor.flags |= DescriptorFlag::has_payload;
    } else {
        descriptor.flags = static_cast<DescriptorFlag>(
            static_cast<std::uint16_t>(descriptor.flags) &
            ~static_cast<std::uint16_t>(DescriptorFlag::has_payload));
    }
    descriptor.data_slot = static_cast<std::uint32_t>(
        position_ % ring_capacity_);
    descriptor.payload_length = static_cast<std::uint32_t>(payload_size);
    descriptor.slot_generation = position_;
    SharedRingSlot* slot = std::exchange(slot_, nullptr);
    slot->descriptor = descriptor;
    slot->generation.store(position_ + 1U, std::memory_order_release);
    payload_ = nullptr;
    return signal_event(event_fd_);
}

std::error_code CompletionReservation::publish_copy(
    IpcDescriptor descriptor, std::span<const std::byte> payload) noexcept {
    if (payload.size() > payload_capacity_ ||
        (payload.data() == nullptr && !payload.empty())) {
        return std::make_error_code(std::errc::message_size);
    }
    if (!payload.empty()) {
        std::memcpy(payload_, payload.data(), payload.size());
    }
    return publish(descriptor, payload.size());
}

void CompletionReservation::abandon() noexcept {
    SharedRingSlot* slot = std::exchange(slot_, nullptr);
    payload_ = nullptr;
    if (slot != nullptr) {
        // Abandonment is only used while tearing down a dead session. Return
        // producer ownership so teardown cannot pin an IPC slot forever.
        slot->generation.store(position_ + ring_capacity_,
                               std::memory_order_release);
    }
}

std::error_code make_error_code(TransportErrc error) noexcept {
    return {static_cast<int>(error), kTransportErrorCategory};
}

struct ClientTransport::Impl {
    SessionResources resources;
    std::atomic<std::uint64_t> next_request_id{1};
    std::uint64_t owner_epoch{process_epoch.load(std::memory_order_relaxed)};
    pid_t owner_pid{::getpid()};

    [[nodiscard]] bool owned_by_current_process() const noexcept {
        return process_owner_matches(owner_epoch, owner_pid);
    }

    ~Impl() {
        if (owned_by_current_process() && resources.header != nullptr) {
            std::uint32_t expected = static_cast<std::uint32_t>(SessionState::active);
            resources.header->state.compare_exchange_strong(
                expected, static_cast<std::uint32_t>(SessionState::closing),
                std::memory_order_acq_rel);
        }
        if (owned_by_current_process() && resources.control) {
            ::shutdown(resources.control.get(), SHUT_RDWR);
        }
    }
};

struct ServerTransport::Impl {
    SessionResources resources;
    std::uint64_t owner_epoch{process_epoch.load(std::memory_order_relaxed)};
    pid_t owner_pid{::getpid()};

    [[nodiscard]] bool owned_by_current_process() const noexcept {
        return process_owner_matches(owner_epoch, owner_pid);
    }

    ~Impl() {
        if (owned_by_current_process() && resources.header != nullptr) {
            resources.header->state.store(
                static_cast<std::uint32_t>(SessionState::dead),
                std::memory_order_release);
        }
        if (owned_by_current_process() && resources.control) {
            ::shutdown(resources.control.get(), SHUT_RDWR);
        }
    }
};

struct ControlServer::Impl {
    struct PendingHandshake {
        UniqueFd control;
        std::chrono::steady_clock::time_point deadline;
    };

    UniqueFd listener;
    std::string path;
    TransportConfig limits{};
    std::vector<PendingHandshake> pending_handshakes;
    std::atomic<std::uint64_t> next_client_id{1};
    dev_t socket_device{0};
    ino_t socket_inode{0};
    std::uint64_t owner_epoch{process_epoch.load(std::memory_order_relaxed)};
    pid_t owner_pid{::getpid()};

    [[nodiscard]] bool owned_by_current_process() const noexcept {
        return process_owner_matches(owner_epoch, owner_pid);
    }

    ~Impl() {
        listener = {};
        struct stat status {};
        if (owned_by_current_process() && !path.empty() &&
            ::lstat(path.c_str(), &status) == 0 &&
            status.st_dev == socket_device && status.st_ino == socket_inode &&
            S_ISSOCK(status.st_mode)) {
            ::unlink(path.c_str());
        }
    }
};

ClientTransport::ClientTransport() noexcept = default;
ClientTransport::~ClientTransport() = default;
ClientTransport::ClientTransport(ClientTransport&&) noexcept = default;
ClientTransport& ClientTransport::operator=(ClientTransport&&) noexcept = default;
ClientTransport::ClientTransport(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

struct ClientConnector::Impl {
    enum class Phase : std::uint8_t {
        connecting,
        sending_hello,
        receiving_hello,
        complete,
    };

    UniqueFd control;
    TransportConfig requested{};
    ClientHello hello{};
    ServerHello response{};
    std::vector<UniqueFd> received_fds;
    Phase phase{Phase::connecting};
};

ClientConnector::ClientConnector() noexcept = default;
ClientConnector::~ClientConnector() = default;
ClientConnector::ClientConnector(ClientConnector&&) noexcept = default;
ClientConnector& ClientConnector::operator=(ClientConnector&&) noexcept =
    default;
ClientConnector::ClientConnector(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

Result<ClientConnector> ClientConnector::start(
    std::string_view socket_path, TransportConfig requested) noexcept {
    if (!valid_config(requested)) {
        return Result<ClientConnector>::failure(
            std::make_error_code(std::errc::invalid_argument));
    }
    sockaddr_un address{};
    socklen_t address_length = 0;
    if (auto error = make_unix_address(socket_path, address, address_length)) {
        return Result<ClientConnector>::failure(error);
    }

    try {
        auto impl = std::make_unique<Impl>();
        impl->requested = requested;
        impl->control = UniqueFd(::socket(
            AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
        if (!impl->control) {
            return Result<ClientConnector>::failure(errno_error());
        }
        if (::connect(impl->control.get(), reinterpret_cast<sockaddr*>(&address),
                      address_length) == 0) {
            impl->phase = Impl::Phase::sending_hello;
        } else if (errno == EINPROGRESS || errno == EALREADY ||
                   errno == EAGAIN || errno == EWOULDBLOCK) {
            impl->phase = Impl::Phase::connecting;
        } else {
            return Result<ClientConnector>::failure(errno_error());
        }
        impl->hello.lane_count = requested.lane_count;
        impl->hello.ring_capacity = requested.ring_capacity;
        impl->hello.data_slot_size = requested.data_slot_size;
        impl->hello.client_pid = static_cast<std::uint32_t>(::getpid());
        impl->hello.client_nonce = new_generation();
        return Result<ClientConnector>::success(
            ClientConnector(std::move(impl)));
    } catch (const std::bad_alloc&) {
        return Result<ClientConnector>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        return Result<ClientConnector>::failure(
            std::make_error_code(std::errc::io_error));
    }
}

Result<std::optional<ClientTransport>> ClientConnector::poll() noexcept {
    if (!impl_ || impl_->phase == Impl::Phase::complete) {
        return Result<std::optional<ClientTransport>>::failure(
            std::make_error_code(std::errc::invalid_argument));
    }
    const auto pending = [] {
        return Result<std::optional<ClientTransport>>::success(
            std::optional<ClientTransport>{});
    };

    if (impl_->phase == Impl::Phase::connecting) {
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if (::getsockopt(impl_->control.get(), SOL_SOCKET, SO_ERROR,
                         &socket_error, &length) != 0) {
            return Result<std::optional<ClientTransport>>::failure(
                errno_error());
        }
        if (socket_error == EINPROGRESS || socket_error == EALREADY ||
            socket_error == EAGAIN || socket_error == EWOULDBLOCK) {
            return pending();
        }
        if (socket_error != 0) {
            return Result<std::optional<ClientTransport>>::failure(
                std::error_code(socket_error, std::generic_category()));
        }
        impl_->phase = Impl::Phase::sending_hello;
    }

    if (impl_->phase == Impl::Phase::sending_hello) {
        const auto error = send_packet(impl_->control.get(), &impl_->hello,
                                       sizeof(impl_->hello));
        if (error == std::errc::resource_unavailable_try_again ||
            error == std::errc::operation_would_block) {
            return pending();
        }
        if (error) {
            return Result<std::optional<ClientTransport>>::failure(error);
        }
        impl_->phase = Impl::Phase::receiving_hello;
    }

    const auto receive_error = receive_packet(
        impl_->control.get(), &impl_->response, sizeof(impl_->response),
        impl_->received_fds);
    if (receive_error == std::errc::resource_unavailable_try_again ||
        receive_error == std::errc::operation_would_block) {
        return pending();
    }
    if (receive_error) {
        return Result<std::optional<ClientTransport>>::failure(receive_error);
    }

    const auto& response = impl_->response;
    if (response.magic != kIpcMagic ||
        response.protocol_version != kIpcProtocolVersion ||
        response.message_size != sizeof(ServerHello)) {
        return Result<std::optional<ClientTransport>>::failure(
            make_error_code(TransportErrc::protocol_mismatch));
    }
    if (response.status != 0) {
        return Result<std::optional<ClientTransport>>::failure(
            std::error_code(response.status, std::generic_category()));
    }
    const TransportConfig negotiated{
        response.lane_count,
        response.ring_capacity,
        response.data_slot_size,
    };
    const std::size_t expected_fds = 1U + 2U * negotiated.lane_count;
    if (!valid_config(negotiated) || negotiated != impl_->requested ||
        response.fd_count != expected_fds ||
        impl_->received_fds.size() != expected_fds ||
        response.shared_memory_size == 0 ||
        response.shared_memory_size > std::numeric_limits<std::size_t>::max()) {
        return Result<std::optional<ClientTransport>>::failure(
            make_error_code(TransportErrc::protocol_mismatch));
    }

    struct stat memory_status {};
    if (::fstat(impl_->received_fds[0].get(), &memory_status) != 0) {
        return Result<std::optional<ClientTransport>>::failure(errno_error());
    }
    if (memory_status.st_size < 0 ||
        static_cast<std::uint64_t>(memory_status.st_size) !=
            response.shared_memory_size) {
        return Result<std::optional<ClientTransport>>::failure(
            make_error_code(TransportErrc::corrupt_layout));
    }
    void* address_ptr = ::mmap(nullptr, response.shared_memory_size,
                               PROT_READ | PROT_WRITE, MAP_SHARED,
                               impl_->received_fds[0].get(), 0);
    if (address_ptr == MAP_FAILED) {
        return Result<std::optional<ClientTransport>>::failure(errno_error());
    }
    Mapping mapping(address_ptr,
                    static_cast<std::size_t>(response.shared_memory_size));

    try {
        auto transport_impl = std::make_unique<ClientTransport::Impl>();
        transport_impl->resources.control = std::move(impl_->control);
        transport_impl->resources.memory_fd =
            std::move(impl_->received_fds[0]);
        transport_impl->resources.mapping = std::move(mapping);
        transport_impl->resources.config = negotiated;
        transport_impl->resources.submission_events.reserve(
            negotiated.lane_count);
        transport_impl->resources.completion_events.reserve(
            negotiated.lane_count);
        for (std::uint32_t lane = 0; lane < negotiated.lane_count; ++lane) {
            transport_impl->resources.submission_events.push_back(
                std::move(impl_->received_fds[1U + 2U * lane]));
            transport_impl->resources.completion_events.push_back(
                std::move(impl_->received_fds[2U + 2U * lane]));
        }
        const auto view_error = build_views(
            transport_impl->resources.mapping, negotiated, response.client_id,
            response.session_generation, transport_impl->resources.lanes,
            transport_impl->resources.header);
        if (view_error) {
            return Result<std::optional<ClientTransport>>::failure(view_error);
        }
        if (transport_impl->resources.header->state.load(
                std::memory_order_acquire) !=
            static_cast<std::uint32_t>(SessionState::active)) {
            return Result<std::optional<ClientTransport>>::failure(
                make_error_code(TransportErrc::stale_session));
        }
        impl_->phase = Impl::Phase::complete;
        return Result<std::optional<ClientTransport>>::success(
            std::optional<ClientTransport>(
                ClientTransport(std::move(transport_impl))));
    } catch (const std::bad_alloc&) {
        return Result<std::optional<ClientTransport>>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        return Result<std::optional<ClientTransport>>::failure(
            std::make_error_code(std::errc::io_error));
    }
}

int ClientConnector::fd() const noexcept {
    return impl_ ? impl_->control.get() : -1;
}

ClientTransport ClientTransport::connect(std::string_view socket_path,
                                         TransportConfig requested,
                                         std::error_code& error) noexcept {
    error.clear();
    auto started = ClientConnector::start(socket_path, requested);
    if (!started) {
        error = started.error();
        return {};
    }
    auto connector = std::move(started).value();
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(kControlHandshakeTimeoutMs);
    for (;;) {
        auto advanced = connector.poll();
        if (!advanced) {
            error = advanced.error();
            return {};
        }
        auto transport = std::move(advanced).value();
        if (transport) {
            return std::move(*transport);
        }
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            error = std::make_error_code(std::errc::timed_out);
            return {};
        }
        const auto remaining = std::chrono::duration_cast<
            std::chrono::milliseconds>(deadline - now);
        pollfd descriptor{connector.fd(), POLLIN | POLLOUT, 0};
        int result = -1;
        do {
            result = ::poll(&descriptor, 1,
                            std::max(1, static_cast<int>(remaining.count())));
        } while (result < 0 && errno == EINTR);
        if (result < 0) {
            error = errno_error();
            return {};
        }
    }
}

std::error_code ClientTransport::try_submit(
    std::uint32_t lane, IpcDescriptor descriptor,
    std::span<const std::byte> payload,
    std::uint64_t* assigned_request_id) noexcept {
    return try_submit_scattered(lane, descriptor, payload, {},
                                assigned_request_id);
}

std::error_code ClientTransport::try_submit_scattered(
    std::uint32_t lane, IpcDescriptor descriptor,
    std::span<const std::byte> prefix, std::span<const std::byte> tail,
    std::uint64_t* assigned_request_id) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    if (impl_->resources.header->state.load(std::memory_order_acquire) !=
        static_cast<std::uint32_t>(SessionState::active)) {
        return TransportErrc::peer_closed;
    }
    if (descriptor.request_id == 0) {
        descriptor.request_id =
            impl_->next_request_id.fetch_add(1, std::memory_order_relaxed);
        if (descriptor.request_id == 0) {
            descriptor.request_id =
                impl_->next_request_id.fetch_add(1, std::memory_order_relaxed);
        }
    }
    descriptor.client_id = impl_->resources.header->client_id;
    descriptor.session_generation =
        impl_->resources.header->session_generation;
    descriptor.protocol_version = kIpcProtocolVersion;
    descriptor.flags = static_cast<DescriptorFlag>(
        (static_cast<std::uint16_t>(descriptor.flags) |
         static_cast<std::uint16_t>(DescriptorFlag::request)) &
        ~static_cast<std::uint16_t>(DescriptorFlag::response));

    LaneView& view = impl_->resources.lanes[lane];
    auto error = view.submissions.try_push(
        descriptor, prefix, tail, view.submission_data,
        impl_->resources.config.data_slot_size);
    if (error) {
        return error;
    }
    if (assigned_request_id != nullptr) {
        *assigned_request_id = descriptor.request_id;
    }
    return signal_event(impl_->resources.submission_events[lane].get());
}

std::error_code ClientTransport::try_receive_completion(
    std::uint32_t lane, IpcDescriptor& descriptor,
    std::span<std::byte> payload_buffer, std::size_t& payload_size) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        payload_size = 0;
        return std::make_error_code(std::errc::invalid_argument);
    }
    LaneView& view = impl_->resources.lanes[lane];
    return view.completions.try_pop(
        descriptor, payload_buffer, payload_size, view.completion_data,
        impl_->resources.config.data_slot_size, impl_->resources.header->client_id,
        impl_->resources.header->session_generation);
}

std::error_code ClientTransport::try_acquire_completion(
    std::uint32_t lane, ReceivedIpcSlot& completion) noexcept {
    completion.reset();
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    AcquiredSlotData acquired;
    LaneView& view = impl_->resources.lanes[lane];
    const auto error = view.completions.try_acquire(
        acquired, view.completion_data,
        impl_->resources.config.data_slot_size,
        impl_->resources.header->client_id,
        impl_->resources.header->session_generation);
    if (error) {
        return error;
    }
    completion = ReceivedIpcSlot(
        acquired.slot, acquired.release_generation, acquired.descriptor,
        acquired.payload);
    return {};
}

std::error_code ClientTransport::drain_completion_notifications(
    std::uint32_t lane, std::uint64_t& notification_count) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        notification_count = 0;
        return std::make_error_code(std::errc::invalid_argument);
    }
    return drain_event(impl_->resources.completion_events[lane].get(),
                       notification_count);
}

int ClientTransport::completion_event_fd(std::uint32_t lane) const noexcept {
    return impl_ && impl_->owned_by_current_process() &&
                   lane < impl_->resources.config.lane_count
               ? impl_->resources.completion_events[lane].get()
               : -1;
}

int ClientTransport::control_fd() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.control.get()
               : -1;
}

bool ClientTransport::peer_alive() const noexcept {
    return impl_ && impl_->owned_by_current_process() &&
           impl_->resources.header->state.load(std::memory_order_acquire) ==
               static_cast<std::uint32_t>(SessionState::active) &&
           socket_peer_alive(impl_->resources.control.get());
}

void ClientTransport::heartbeat() noexcept {
    if (impl_ && impl_->owned_by_current_process()) {
        impl_->resources.header->heartbeat.fetch_add(1, std::memory_order_release);
    }
}

ClientTransport::operator bool() const noexcept {
    return impl_ != nullptr && impl_->owned_by_current_process();
}

std::uint64_t ClientTransport::client_id() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.header->client_id
               : 0;
}

std::uint64_t ClientTransport::session_generation() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.header->session_generation
               : 0;
}

TransportConfig ClientTransport::config() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.config
               : TransportConfig{};
}

ServerTransport::ServerTransport() noexcept = default;
ServerTransport::~ServerTransport() = default;
ServerTransport::ServerTransport(ServerTransport&&) noexcept = default;
ServerTransport& ServerTransport::operator=(ServerTransport&&) noexcept = default;
ServerTransport::ServerTransport(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

std::error_code ServerTransport::try_receive_submission(
    std::uint32_t lane, IpcDescriptor& descriptor,
    std::span<std::byte> payload_buffer, std::size_t& payload_size) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        payload_size = 0;
        return std::make_error_code(std::errc::invalid_argument);
    }
    LaneView& view = impl_->resources.lanes[lane];
    return view.submissions.try_pop(
        descriptor, payload_buffer, payload_size, view.submission_data,
        impl_->resources.config.data_slot_size, impl_->resources.header->client_id,
        impl_->resources.header->session_generation);
}

bool ServerTransport::submission_ready(std::uint32_t lane) const noexcept {
    return impl_ && impl_->owned_by_current_process() &&
           lane < impl_->resources.config.lane_count &&
           impl_->resources.lanes[lane].submissions.ready();
}

std::error_code ServerTransport::try_acquire_submission(
    std::uint32_t lane, ReceivedIpcSlot& submission) noexcept {
    submission.reset();
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    AcquiredSlotData acquired;
    LaneView& view = impl_->resources.lanes[lane];
    const auto error = view.submissions.try_acquire(
        acquired, view.submission_data,
        impl_->resources.config.data_slot_size,
        impl_->resources.header->client_id,
        impl_->resources.header->session_generation);
    if (error) {
        return error;
    }
    submission = ReceivedIpcSlot(
        acquired.slot, acquired.release_generation, acquired.descriptor,
        acquired.payload);
    return {};
}

std::error_code ServerTransport::try_reserve_completion(
    std::uint32_t lane, CompletionReservation& completion) noexcept {
    completion.abandon();
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    ReservedSlotData reserved;
    LaneView& view = impl_->resources.lanes[lane];
    const auto error = view.completions.try_reserve(
        reserved, view.completion_data,
        impl_->resources.config.data_slot_size);
    if (error) {
        return error;
    }
    completion = CompletionReservation(
        reserved.slot, reserved.payload,
        impl_->resources.config.data_slot_size, reserved.position,
        impl_->resources.config.ring_capacity,
        impl_->resources.completion_events[lane].get(),
        impl_->resources.header->client_id,
        impl_->resources.header->session_generation);
    return {};
}

std::error_code ServerTransport::try_complete(
    std::uint32_t lane, IpcDescriptor descriptor,
    std::span<const std::byte> payload) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    if (descriptor.client_id != impl_->resources.header->client_id ||
        descriptor.session_generation !=
            impl_->resources.header->session_generation ||
        descriptor.request_id == 0) {
        return TransportErrc::stale_session;
    }
    descriptor.protocol_version = kIpcProtocolVersion;
    descriptor.flags = static_cast<DescriptorFlag>(
        (static_cast<std::uint16_t>(descriptor.flags) |
         static_cast<std::uint16_t>(DescriptorFlag::response)) &
        ~static_cast<std::uint16_t>(DescriptorFlag::request));
    LaneView& view = impl_->resources.lanes[lane];
    auto error = view.completions.try_push(
        descriptor, payload, {}, view.completion_data,
        impl_->resources.config.data_slot_size);
    if (error) {
        return error;
    }
    return signal_event(impl_->resources.completion_events[lane].get());
}

std::error_code ServerTransport::drain_submission_notifications(
    std::uint32_t lane, std::uint64_t& notification_count) noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        lane >= impl_->resources.config.lane_count) {
        notification_count = 0;
        return std::make_error_code(std::errc::invalid_argument);
    }
    return drain_event(impl_->resources.submission_events[lane].get(),
                       notification_count);
}

int ServerTransport::submission_event_fd(std::uint32_t lane) const noexcept {
    return impl_ && impl_->owned_by_current_process() &&
                   lane < impl_->resources.config.lane_count
               ? impl_->resources.submission_events[lane].get()
               : -1;
}

int ServerTransport::control_fd() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.control.get()
               : -1;
}

bool ServerTransport::peer_alive() const noexcept {
    return impl_ && impl_->owned_by_current_process() &&
           impl_->resources.header->state.load(std::memory_order_acquire) ==
               static_cast<std::uint32_t>(SessionState::active) &&
           socket_peer_alive(impl_->resources.control.get());
}

void ServerTransport::mark_dead() noexcept {
    if (impl_ && impl_->owned_by_current_process()) {
        impl_->resources.header->state.store(
            static_cast<std::uint32_t>(SessionState::dead),
            std::memory_order_release);
        ::shutdown(impl_->resources.control.get(), SHUT_RDWR);
    }
}

ServerTransport::operator bool() const noexcept {
    return impl_ != nullptr && impl_->owned_by_current_process();
}

std::uint64_t ServerTransport::client_id() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.header->client_id
               : 0;
}

std::uint64_t ServerTransport::session_generation() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.header->session_generation
               : 0;
}

std::uint32_t ServerTransport::client_pid() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.header->client_pid
               : 0;
}

TransportConfig ServerTransport::config() const noexcept {
    return impl_ && impl_->owned_by_current_process()
               ? impl_->resources.config
               : TransportConfig{};
}

SharedMemoryRegion ServerTransport::shared_memory_region() const noexcept {
    if (!impl_ || !impl_->owned_by_current_process() ||
        impl_->resources.mapping.address == MAP_FAILED) {
        return {};
    }
    return SharedMemoryRegion{
        .address = impl_->resources.mapping.address,
        .size = impl_->resources.mapping.size,
        .hugepage_backed = impl_->resources.hugepage_backed,
    };
}

ControlServer::ControlServer() noexcept = default;
ControlServer::~ControlServer() = default;
ControlServer::ControlServer(ControlServer&&) noexcept = default;
ControlServer& ControlServer::operator=(ControlServer&&) noexcept = default;
ControlServer::ControlServer(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

ControlServer ControlServer::listen(std::string_view socket_path,
                                    TransportConfig limits,
                                    std::error_code& error) noexcept {
    error.clear();
    if (!valid_config(limits)) {
        error = std::make_error_code(std::errc::invalid_argument);
        return {};
    }
    sockaddr_un address{};
    socklen_t address_length = 0;
    error = make_unix_address(socket_path, address, address_length);
    if (error) {
        return {};
    }

    UniqueFd listener(
        ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC | SOCK_NONBLOCK, 0));
    if (!listener) {
        error = errno_error();
        return {};
    }
    if (::bind(listener.get(), reinterpret_cast<sockaddr*>(&address),
               address_length) != 0) {
        error = errno_error();
        return {};
    }
    const std::string path(socket_path);
    if (::listen(listener.get(), 128) != 0) {
        error = errno_error();
        ::unlink(path.c_str());
        return {};
    }
    struct stat status {};
    if (::lstat(path.c_str(), &status) != 0) {
        error = errno_error();
        ::unlink(path.c_str());
        return {};
    }

    auto impl = std::make_unique<Impl>();
    impl->listener = std::move(listener);
    impl->path = path;
    impl->limits = limits;
    impl->socket_device = status.st_dev;
    impl->socket_inode = status.st_ino;
    return ControlServer(std::move(impl));
}

ServerTransport ControlServer::accept(std::error_code& error,
                                      int cancellation_fd) noexcept {
    error.clear();
    if (!impl_ || !impl_->owned_by_current_process()) {
        error = std::make_error_code(std::errc::bad_file_descriptor);
        return {};
    }
    std::array<pollfd, 2> descriptors{{
        {impl_->listener.get(), POLLIN, 0},
        {cancellation_fd, POLLIN, 0},
    }};
    const nfds_t descriptor_count = cancellation_fd >= 0 ? 2 : 1;
    int poll_result = -1;
    do {
        poll_result = ::poll(descriptors.data(), descriptor_count, -1);
    } while (poll_result < 0 && errno == EINTR);
    if (poll_result < 0) {
        error = errno_error();
        return {};
    }
    if (cancellation_fd >= 0 && descriptors[1].revents != 0) {
        error = std::make_error_code(std::errc::operation_canceled);
        return {};
    }
    if ((descriptors[0].revents & POLLIN) == 0) {
        error = std::make_error_code(std::errc::io_error);
        return {};
    }
    int accepted_fd = -1;
    do {
        accepted_fd = ::accept4(impl_->listener.get(), nullptr, nullptr,
                                SOCK_CLOEXEC | SOCK_NONBLOCK);
    } while (accepted_fd < 0 && errno == EINTR);
    if (accepted_fd < 0) {
        error = errno_error();
        return {};
    }
    UniqueFd pending_control(accepted_fd);
    std::array<pollfd, 2> handshake_descriptors{{
        {pending_control.get(), POLLIN, 0},
        {cancellation_fd, POLLIN, 0},
    }};
    const nfds_t handshake_count = cancellation_fd >= 0 ? 2 : 1;
    int handshake_result = -1;
    do {
        handshake_result = ::poll(handshake_descriptors.data(), handshake_count,
                                  kControlHandshakeTimeoutMs);
    } while (handshake_result < 0 && errno == EINTR);
    if (handshake_result < 0) {
        error = errno_error();
        return {};
    }
    if (handshake_result == 0) {
        error = std::make_error_code(std::errc::timed_out);
        return {};
    }
    if (cancellation_fd >= 0 && handshake_descriptors[1].revents != 0) {
        error = std::make_error_code(std::errc::operation_canceled);
        return {};
    }
    if ((handshake_descriptors[0].revents & POLLIN) == 0) {
        error = TransportErrc::peer_closed;
        return {};
    }

    return accept_connected(pending_control.release(), error);
}

ServerTransport ControlServer::try_accept(std::error_code& error) noexcept {
    error.clear();
    if (!impl_ || !impl_->owned_by_current_process()) {
        error = std::make_error_code(std::errc::bad_file_descriptor);
        return {};
    }

    const auto now = std::chrono::steady_clock::now();
    std::erase_if(impl_->pending_handshakes, [&](const auto& pending) {
        return pending.deadline <= now;
    });

    for (;;) {
        int accepted_fd = -1;
        do {
            accepted_fd = ::accept4(impl_->listener.get(), nullptr, nullptr,
                                    SOCK_CLOEXEC | SOCK_NONBLOCK);
        } while (accepted_fd < 0 && errno == EINTR);
        if (accepted_fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                error = errno_error();
                return {};
            }
            break;
        }
        UniqueFd control(accepted_fd);
        try {
            impl_->pending_handshakes.push_back({
                std::move(control),
                now + std::chrono::milliseconds(kControlHandshakeTimeoutMs),
            });
        } catch (const std::bad_alloc&) {
            error = std::make_error_code(std::errc::not_enough_memory);
            return {};
        } catch (...) {
            std::terminate();
        }
    }

    for (auto pending = impl_->pending_handshakes.begin();
         pending != impl_->pending_handshakes.end();) {
        pollfd descriptor{pending->control.get(), POLLIN, 0};
        int poll_result = -1;
        do {
            poll_result = ::poll(&descriptor, 1, 0);
        } while (poll_result < 0 && errno == EINTR);
        if (poll_result < 0) {
            error = errno_error();
            return {};
        }
        if (poll_result == 0) {
            ++pending;
            continue;
        }
        if ((descriptor.revents & POLLIN) == 0) {
            pending = impl_->pending_handshakes.erase(pending);
            continue;
        }

        const int accepted_fd = pending->control.release();
        pending = impl_->pending_handshakes.erase(pending);
        return accept_connected(accepted_fd, error);
    }

    error = std::make_error_code(std::errc::resource_unavailable_try_again);
    return {};
}

ServerTransport ControlServer::accept_connected(
    int accepted_fd, std::error_code& error) noexcept {
    UniqueFd control(accepted_fd);

    ClientHello hello{};
    std::vector<UniqueFd> unexpected_fds;
    error = receive_packet(control.get(), &hello, sizeof(hello), unexpected_fds);
    if (error) {
        return {};
    }
    ServerHello response{};
    const auto reject = [&](int status) {
        response.status = status;
        response.fd_count = 0;
        (void)send_packet(control.get(), &response, sizeof(response));
    };
    if (!unexpected_fds.empty() || hello.magic != kIpcMagic ||
        hello.message_size != sizeof(ClientHello)) {
        reject(EPROTO);
        error = TransportErrc::protocol_mismatch;
        return {};
    }
    if (hello.protocol_version != kIpcProtocolVersion) {
        reject(EPROTONOSUPPORT);
        error = TransportErrc::protocol_mismatch;
        return {};
    }
    const TransportConfig requested{hello.lane_count, hello.ring_capacity,
                                    hello.data_slot_size};
    if (!valid_config(requested) ||
        requested.lane_count > impl_->limits.lane_count ||
        requested.ring_capacity > impl_->limits.ring_capacity ||
        requested.data_slot_size > impl_->limits.data_slot_size) {
        reject(EINVAL);
        error = std::make_error_code(std::errc::invalid_argument);
        return {};
    }

    struct ucred credentials {};
    socklen_t credential_size = sizeof(credentials);
    if (::getsockopt(control.get(), SOL_SOCKET, SO_PEERCRED, &credentials,
                     &credential_size) != 0) {
        reject(EACCES);
        error = errno_error();
        return {};
    }
    if (credentials.pid <= 0 ||
        hello.client_pid != static_cast<std::uint32_t>(credentials.pid)) {
        reject(EACCES);
        error = std::make_error_code(std::errc::permission_denied);
        return {};
    }

    SessionLayout layout{};
    error = make_layout(requested, layout);
    if (error) {
        reject(error.value());
        return {};
    }
    bool hugepage_backed = false;
    UniqueFd memory_fd(create_memfd(hugepage_backed));
    if (!memory_fd) {
        error = errno_error();
        reject(error.value());
        return {};
    }
    if (::ftruncate(memory_fd.get(), static_cast<off_t>(layout.total_size)) != 0) {
        error = errno_error();
        reject(error.value());
        return {};
    }
    if (::fcntl(memory_fd.get(), F_ADD_SEALS,
                F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL) != 0) {
        error = errno_error();
        reject(error.value());
        return {};
    }
    void* mapped = ::mmap(nullptr, layout.total_size, PROT_READ | PROT_WRITE,
                          MAP_SHARED, memory_fd.get(), 0);
    if (mapped == MAP_FAILED) {
        error = errno_error();
        reject(error.value());
        return {};
    }
    Mapping mapping(mapped, static_cast<std::size_t>(layout.total_size));

    const std::uint64_t client_id =
        impl_->next_client_id.fetch_add(1, std::memory_order_relaxed);
    std::uint64_t generation = new_generation() ^ hello.client_nonce;
    if (generation == 0) {
        generation = new_generation();
    }
    error = initialize_mapping(mapped, layout, requested, client_id, generation,
                               static_cast<std::uint32_t>(credentials.pid));
    if (error) {
        reject(error.value());
        return {};
    }

    std::vector<UniqueFd> submission_events;
    std::vector<UniqueFd> completion_events;
    submission_events.reserve(requested.lane_count);
    completion_events.reserve(requested.lane_count);
    for (std::uint32_t lane = 0; lane < requested.lane_count; ++lane) {
        UniqueFd submission(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
        UniqueFd completion(::eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK));
        if (!submission || !completion) {
            error = errno_error();
            reject(error.value());
            return {};
        }
        submission_events.push_back(std::move(submission));
        completion_events.push_back(std::move(completion));
    }

    auto server_impl = std::make_unique<ServerTransport::Impl>();
    server_impl->resources.control = std::move(control);
    server_impl->resources.memory_fd = std::move(memory_fd);
    server_impl->resources.mapping = std::move(mapping);
    server_impl->resources.submission_events = std::move(submission_events);
    server_impl->resources.completion_events = std::move(completion_events);
    server_impl->resources.config = requested;
    server_impl->resources.hugepage_backed = hugepage_backed;
    error = build_views(server_impl->resources.mapping, requested, client_id,
                        generation, server_impl->resources.lanes,
                        server_impl->resources.header);
    if (error) {
        reject(error.value());
        return {};
    }
    server_impl->resources.header->state.store(
        static_cast<std::uint32_t>(SessionState::active),
        std::memory_order_release);

    std::vector<int> descriptors;
    descriptors.reserve(1U + 2U * requested.lane_count);
    descriptors.push_back(server_impl->resources.memory_fd.get());
    for (std::uint32_t lane = 0; lane < requested.lane_count; ++lane) {
        descriptors.push_back(server_impl->resources.submission_events[lane].get());
        descriptors.push_back(server_impl->resources.completion_events[lane].get());
    }
    response.lane_count = requested.lane_count;
    response.ring_capacity = requested.ring_capacity;
    response.data_slot_size = requested.data_slot_size;
    response.fd_count = static_cast<std::uint32_t>(descriptors.size());
    response.shared_memory_size = layout.total_size;
    response.client_id = client_id;
    response.session_generation = generation;
    error = send_packet(server_impl->resources.control.get(), &response,
                        sizeof(response), descriptors);
    if (error) {
        return {};
    }
    return ServerTransport(std::move(server_impl));
}

int ControlServer::fd() const noexcept {
    return impl_ && impl_->owned_by_current_process() ? impl_->listener.get()
                                                      : -1;
}

ControlServer::operator bool() const noexcept {
    return impl_ != nullptr && impl_->owned_by_current_process();
}

}  // namespace orchfs::async
