#pragma once

#include "orchfs/async/result.hpp"

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace orchfs::async {

inline constexpr std::uint32_t kRpcSchemaVersion = 1;
inline constexpr std::size_t kRpcNameCapacity = 232;

using RemoteHandle = std::uint64_t;
inline constexpr RemoteHandle kInvalidRemoteHandle = 0;

enum class RpcRequestFlag : std::uint32_t {
  none = 0,
  implicit_offset = 1U << 0,
  data_follows = 1U << 1,
};

constexpr RpcRequestFlag operator|(RpcRequestFlag lhs,
                                   RpcRequestFlag rhs) noexcept {
  return static_cast<RpcRequestFlag>(static_cast<std::uint32_t>(lhs) |
                                     static_cast<std::uint32_t>(rhs));
}

// The descriptor selects the operation.  This fixed prefix carries the
// operation-specific scalar arguments; path bytes or write data follow it.
// All fields use host byte order because the two peers are local processes.
struct RpcRequest {
  std::uint32_t schema_version{kRpcSchemaVersion};
  std::uint32_t flags{};
  RemoteHandle handle{kInvalidRemoteHandle};
  RemoteHandle second_handle{kInvalidRemoteHandle};
  std::int64_t offset{};
  std::uint64_t length{};
  std::int64_t value{};
  std::int32_t open_flags{};
  std::uint32_t mode{};
  std::int32_t whence{};
  std::uint32_t path1_length{};
  std::uint32_t path2_length{};
  std::uint32_t data_length{};
  std::uint32_t reserved{};
};

struct RpcResponse {
  std::uint32_t schema_version{kRpcSchemaVersion};
  std::uint32_t flags{};
  RemoteHandle handle{kInvalidRemoteHandle};
  std::int64_t value{};
  std::uint64_t data_length{};
};

// Stable wire representations.  Do not copy libc structs through shared
// memory: their layout varies with libc and compilation flags.
struct RpcFileStat {
#define ORCHFS_ASYNC_FILE_STAT_FIELD(type, name, posix_name) type name{};
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILE_STAT_FIELD
};

struct RpcStatFs {
#define ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(type, name, posix_name) type name{};
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD
};

struct RpcDirEntry {
  std::uint64_t inode{};
  std::int64_t offset{};
  std::uint16_t record_length{};
  std::uint8_t type{};
  std::uint8_t name_length{};
  std::array<char, kRpcNameCapacity> name{};
};

static_assert(std::is_trivially_copyable_v<RpcRequest>);
static_assert(std::is_trivially_copyable_v<RpcResponse>);
static_assert(std::is_trivially_copyable_v<RpcFileStat>);
static_assert(std::is_trivially_copyable_v<RpcStatFs>);
static_assert(std::is_trivially_copyable_v<RpcDirEntry>);

namespace detail {

template <typename T>
[[gnu::always_inline]] inline void append_object(
    std::vector<std::byte>& bytes, const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  const auto old_size = bytes.size();
  bytes.resize(old_size + sizeof(T));
  std::memcpy(bytes.data() + old_size, &value, sizeof(T));
}

[[gnu::always_inline]] inline void append_bytes(std::vector<std::byte>& bytes,
                                                const void* data,
                                                std::size_t size) {
  if (size == 0) {
    return;
  }
  const auto old_size = bytes.size();
  bytes.resize(old_size + size);
  std::memcpy(bytes.data() + old_size, data, size);
}

template <typename T>
[[nodiscard, gnu::always_inline]] inline Result<T>
decode_object(std::span<const std::byte> bytes) {
  static_assert(std::is_trivially_copyable_v<T>);
  if (bytes.size() != sizeof(T)) {
    return Result<T>::failure(
        std::make_error_code(std::errc::protocol_error));
  }
  T value{};
  std::memcpy(&value, bytes.data(), sizeof(T));
  return Result<T>::success(std::move(value));
}

template <typename T>
[[nodiscard, gnu::always_inline]] inline std::vector<std::byte>
encode_object(const T& value) {
  static_assert(std::is_trivially_copyable_v<T>);
  std::vector<std::byte> bytes(sizeof(T));
  std::memcpy(bytes.data(), &value, sizeof(T));
  return bytes;
}

}  // namespace detail

}  // namespace orchfs::async
