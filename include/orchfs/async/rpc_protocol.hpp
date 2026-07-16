#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <type_traits>

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
  std::uint64_t device{};
  std::uint64_t inode{};
  std::uint64_t mode{};
  std::uint64_t link_count{};
  std::uint64_t uid{};
  std::uint64_t gid{};
  std::uint64_t rdev{};
  std::int64_t size{};
  std::int64_t block_size{};
  std::int64_t blocks{};
  std::int64_t atime_seconds{};
  std::int64_t atime_nanoseconds{};
  std::int64_t mtime_seconds{};
  std::int64_t mtime_nanoseconds{};
  std::int64_t ctime_seconds{};
  std::int64_t ctime_nanoseconds{};
};

struct RpcStatFs {
  std::uint64_t type{};
  std::uint64_t block_size{};
  std::uint64_t blocks{};
  std::uint64_t blocks_free{};
  std::uint64_t blocks_available{};
  std::uint64_t files{};
  std::uint64_t files_free{};
  std::uint64_t name_length{};
  std::uint64_t fragment_size{};
  std::uint64_t flags{};
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

}  // namespace orchfs::async
