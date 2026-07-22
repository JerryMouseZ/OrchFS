#pragma once

#include "orchfs/async/result.hpp"
#include "orchfs/async/task.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

namespace orchfs::async {

using InodeNumber = std::int64_t;

enum class NodeType : std::uint8_t {
  unknown = 0,
  directory = 1,
  regular = 2,
};

struct OpenedNode {
  InodeNumber inode{-1};
  NodeType type{NodeType::unknown};
};

struct FileStat {
#define ORCHFS_ASYNC_FILE_STAT_FIELD(type, name, posix_name) type name{};
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILE_STAT_FIELD
};

struct FileSystemStat {
#define ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(type, name, posix_name) type name{};
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD
};

struct DirEntry {
  std::uint64_t inode{};
  std::int64_t offset{};
  std::uint8_t type{};
  std::string name;
};

struct WriteResult {
  std::size_t bytes{};
  // The actual first byte written. This differs from the requested offset for
  // append and lets the server publish the descriptor offset atomically with
  // the inode-size decision made inside the filesystem module.
  std::uint64_t offset{};
};

struct DirectoryReadResult {
  std::size_t count{};
  std::uint64_t next_cursor{};
};

// Runtime-native filesystem module. Implementations own all inode, namespace,
// allocator, journal, and data-range serialization. Buffers passed through the
// interface remain borrowed until the returned Task completes.
class AsyncFilesystem {
 public:
  virtual ~AsyncFilesystem() = default;

  AsyncFilesystem(const AsyncFilesystem&) = delete;
  AsyncFilesystem& operator=(const AsyncFilesystem&) = delete;

  [[nodiscard]] virtual Task<Result<OpenedNode>> open(
      std::string path, int flags, std::uint32_t mode) = 0;
  [[nodiscard]] virtual Task<Result<OpenedNode>> open_at(
      InodeNumber directory, std::string path, int flags,
      std::uint32_t mode) = 0;
  [[nodiscard]] virtual Task<Result<void>> close(OpenedNode node) = 0;

  [[nodiscard]] virtual Task<Result<std::size_t>> read(
      InodeNumber inode, std::uint64_t offset,
      std::span<std::byte> destination) = 0;
  [[nodiscard]] virtual Task<Result<WriteResult>> write(
      InodeNumber inode, std::uint64_t offset,
      std::span<const std::byte> source, bool append) = 0;

  [[nodiscard]] virtual Task<Result<FileStat>> stat(std::string path) = 0;
  [[nodiscard]] virtual Task<Result<FileStat>> stat(InodeNumber inode) = 0;
  [[nodiscard]] virtual Task<Result<FileSystemStat>> statfs(
      InodeNumber inode) = 0;
  [[nodiscard]] virtual Task<Result<std::uint64_t>> seek(
      InodeNumber inode, std::uint64_t current_offset,
      std::int64_t offset, int whence) = 0;
  [[nodiscard]] virtual Task<Result<void>> truncate(
      std::string path, std::uint64_t size) = 0;
  [[nodiscard]] virtual Task<Result<void>> truncate(
      InodeNumber inode, std::uint64_t size) = 0;
  [[nodiscard]] virtual Task<Result<void>> sync(InodeNumber inode) = 0;

  [[nodiscard]] virtual Task<Result<void>> make_directory(
      std::string path, std::uint32_t mode) = 0;
  [[nodiscard]] virtual Task<Result<void>> remove_directory(
      std::string path) = 0;
  [[nodiscard]] virtual Task<Result<void>> unlink(std::string path) = 0;
  [[nodiscard]] virtual Task<Result<void>> rename(
      std::string old_path, std::string new_path) = 0;

  [[nodiscard]] virtual Task<Result<OpenedNode>> open_directory(
      std::string path) = 0;
  [[nodiscard]] virtual Task<Result<OpenedNode>> open_directory(
      InodeNumber inode) = 0;
  [[nodiscard]] virtual Task<Result<DirectoryReadResult>> read_directory(
      InodeNumber inode, std::uint64_t cursor,
      std::span<DirEntry> entries) = 0;
  [[nodiscard]] virtual Task<Result<void>> close_directory(
      OpenedNode node) = 0;

 protected:
  AsyncFilesystem() = default;
};

}  // namespace orchfs::async
