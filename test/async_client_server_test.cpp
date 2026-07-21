#include "orchfs/async/client.hpp"
#include "orchfs/async/filesystem.hpp"
#include "orchfs/async/ipc_transport.hpp"
#include "orchfs/async/rpc_protocol.hpp"
#include "orchfs/async/runtime.hpp"
#include "orchfs/async/server.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdarg>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <dlfcn.h>
#include <filesystem>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>
#include <poll.h>

extern "C" {
int orchfs_open(const char* pathname, int flags, ...);
int orchfs_openat(int dirfd, const char* pathname, int flags, ...);
int orchfs_close(int fd);
std::int64_t orchfs_pwrite(int fd, const void* buffer, std::int64_t length,
                           std::int64_t offset);
std::int64_t orchfs_pread(int fd, void* buffer, std::int64_t length,
                          std::int64_t offset);
int orchfs_mkdir(const char* pathname, std::uint16_t mode);
int orchfs_rmdir(const char* pathname);
int orchfs_unlink(const char* pathname);
int orchfs_fstatfs(int fd, struct statfs* value);
int orchfs_stat(const char* pathname, struct stat* value);
int orchfs_fstat(int fd, struct stat* value);
int orchfs_truncate(const char* pathname, std::size_t length);
int orchfs_ftruncate(int fd, std::size_t length);
int orchfs_fsync(int fd);
int orchfs_rename(const char* old_path, const char* new_path);
}

namespace {

std::string root_path;
std::atomic<std::size_t> legacy_call_count{0};
std::atomic<bool> legacy_called_on_runtime{false};
std::atomic<std::size_t> namespace_handle_call_count{0};
std::atomic<bool> namespace_handle_called_off_owner{false};
std::atomic<std::size_t> lifecycle_call_count{0};
std::atomic<bool> lifecycle_called_off_owner{false};
std::atomic<std::size_t> inode_owner_call_count{0};
std::atomic<bool> inode_called_off_owner{false};
std::mutex file_handle_mutex;
std::unordered_map<int, std::string> file_handle_paths;
std::unordered_set<int> close_failed_once;
std::atomic<std::size_t> active_legacy_handles{0};

// Opt-in integration benchmark. It measures the coroutine Runtime, shared-memory
// RPC, server dispatch, and blocking-adapter path against /tmp; it is not an
// SPDK media benchmark and is intentionally excluded from ordinary CTest runs.
constexpr std::size_t benchmark_block_size = 64U * 1024U;
constexpr std::size_t benchmark_stream_count = 4;
constexpr std::size_t benchmark_warmup_operations = 64;
constexpr std::size_t benchmark_timed_operations = 4096;

void record_legacy_call() {
  legacy_call_count.fetch_add(1, std::memory_order_relaxed);
}

void record_host_io_call() {
  record_legacy_call();
  if (orchfs::async::Runtime::current() != nullptr) {
    legacy_called_on_runtime.store(true, std::memory_order_relaxed);
  }
}

void record_namespace_handle_call() {
  namespace_handle_call_count.fetch_add(1, std::memory_order_relaxed);
  if (orchfs::async::Runtime::current_worker() != 0) {
    namespace_handle_called_off_owner.store(true, std::memory_order_relaxed);
  }
}

void record_lifecycle_call() {
  lifecycle_call_count.fetch_add(1, std::memory_order_relaxed);
  if (orchfs::async::Runtime::current_worker() != 0) {
    lifecycle_called_off_owner.store(true, std::memory_order_relaxed);
  }
}

void record_inode_owner_call(orchfs::async::InodeNumber inode) {
  inode_owner_call_count.fetch_add(1, std::memory_order_relaxed);
  auto* runtime = orchfs::async::Runtime::current();
  if (runtime == nullptr || orchfs::async::Runtime::current_worker() !=
                                runtime->owner_for(
                                    static_cast<std::uint64_t>(inode))) {
    inode_called_off_owner.store(true, std::memory_order_relaxed);
  }
}

std::string host_path(const char* path) {
  std::string_view view(path == nullptr ? "" : path);
  while (!view.empty() && view.front() == '/') {
    view.remove_prefix(1);
  }
  return root_path + "/" + std::string(view);
}

void remember_file_handle(int fd, std::string path) {
  if (fd < 0) {
    return;
  }
  {
    std::lock_guard lock(file_handle_mutex);
    file_handle_paths.insert_or_assign(fd, std::move(path));
  }
  active_legacy_handles.fetch_add(1, std::memory_order_relaxed);
}

bool file_path_is(int fd, std::string_view expected) {
  std::lock_guard lock(file_handle_mutex);
  const auto found = file_handle_paths.find(fd);
  return found != file_handle_paths.end() && found->second == expected;
}

bool fail_this_close_once(int fd) {
  std::lock_guard lock(file_handle_mutex);
  const auto found = file_handle_paths.find(fd);
  return found != file_handle_paths.end() && found->second == "/close-retry" &&
         close_failed_once.insert(fd).second;
}

int legacy_open_flags(int flags) noexcept {
  int ignored = O_TRUNC | O_APPEND;
#ifdef O_DIRECTORY
  // The production legacy core ignores O_DIRECTORY. Keep the test double from
  // hiding the server-side fd type validation behind host open(2) behavior.
  ignored |= O_DIRECTORY;
#endif
  return flags & ~ignored;
}

void forget_file_handle(int fd) {
  bool erased = false;
  {
    std::lock_guard lock(file_handle_mutex);
    erased = file_handle_paths.erase(fd) != 0;
    close_failed_once.erase(fd);
  }
  if (erased) {
    active_legacy_handles.fetch_sub(1, std::memory_order_relaxed);
  }
}

template <typename T>
T run(orchfs::async::Runtime& runtime,
      orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    throw std::system_error(submitted.error());
  }
  auto handle = std::move(submitted).value();
  auto joined = std::move(handle).join();
  if (!joined) {
    throw std::system_error(joined.error());
  }
  auto result = std::move(joined).value();
  if (!result) {
    throw std::system_error(result.error());
  }
  return std::move(result).value();
}

void run(orchfs::async::Runtime& runtime,
         orchfs::async::Task<orchfs::async::Result<void>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    throw std::system_error(submitted.error());
  }
  auto handle = std::move(submitted).value();
  auto joined = std::move(handle).join();
  if (!joined) {
    throw std::system_error(joined.error());
  }
  auto result = std::move(joined).value();
  if (!result) {
    throw std::system_error(result.error());
  }
}

template <typename T>
std::error_code run_error(
    orchfs::async::Runtime& runtime,
    orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    return submitted.error();
  }
  auto joined = std::move(submitted).value().join();
  if (!joined) {
    return joined.error();
  }
  auto result = std::move(joined).value();
  return result ? std::error_code{} : result.error();
}

void require(bool condition, std::string_view message) {
  if (!condition) {
    throw std::runtime_error(std::string(message));
  }
}

class HostAsyncFilesystem final : public orchfs::async::AsyncFilesystem {
 public:
  orchfs::async::Task<orchfs::async::Result<orchfs::async::OpenedNode>> open(
      std::string path, int flags, std::uint32_t mode) override {
    co_return open_impl(-1, std::move(path), flags, mode);
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::OpenedNode>> open_at(
      orchfs::async::InodeNumber directory, std::string path, int flags,
      std::uint32_t mode) override {
    record_namespace_handle_call();
    co_return open_impl(directory, std::move(path), flags, mode);
  }

  orchfs::async::Task<orchfs::async::Result<void>> close(
      orchfs::async::OpenedNode node) override {
    record_lifecycle_call();
    co_return close_impl(node.inode);
  }

  orchfs::async::Task<orchfs::async::Result<std::size_t>> read(
      orchfs::async::InodeNumber inode, std::uint64_t offset,
      std::span<std::byte> destination) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    if (fd < 0) {
      co_return failure<std::size_t>(EBADF);
    }
    const auto result = orchfs_pread(
        fd, destination.data(), static_cast<std::int64_t>(destination.size()),
        static_cast<std::int64_t>(offset));
    if (result < 0) {
      co_return failure<std::size_t>(current_error(EIO));
    }
    co_return orchfs::async::Result<std::size_t>::success(
        static_cast<std::size_t>(result));
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::WriteResult>> write(
      orchfs::async::InodeNumber inode, std::uint64_t offset,
      std::span<const std::byte> source, bool append) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    if (fd < 0) {
      co_return failure<orchfs::async::WriteResult>(EBADF);
    }
    struct stat value {};
    if (orchfs_fstat(fd, &value) != 0) {
      co_return failure<orchfs::async::WriteResult>(current_error(EIO));
    }
    const auto actual = append ? static_cast<std::uint64_t>(value.st_size)
                               : offset;
    if (actual > static_cast<std::uint64_t>(value.st_size)) {
      co_return orchfs::async::Result<orchfs::async::WriteResult>::success(
          {.bytes = 0, .offset = actual});
    }
    const auto result = orchfs_pwrite(
        fd, source.data(), static_cast<std::int64_t>(source.size()),
        static_cast<std::int64_t>(actual));
    if (result < 0) {
      co_return failure<orchfs::async::WriteResult>(current_error(EIO));
    }
    co_return orchfs::async::Result<orchfs::async::WriteResult>::success({
        .bytes = static_cast<std::size_t>(result),
        .offset = actual,
    });
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::FileStat>> stat(
      std::string path) override {
    struct stat value {};
    if (orchfs_stat(path.c_str(), &value) != 0) {
      co_return failure<orchfs::async::FileStat>(current_error(ENOENT));
    }
    co_return orchfs::async::Result<orchfs::async::FileStat>::success(
        to_stat(value));
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::FileStat>> stat(
      orchfs::async::InodeNumber inode) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    struct stat value {};
    if (fd < 0) {
      co_return failure<orchfs::async::FileStat>(EBADF);
    }
    if (orchfs_fstat(fd, &value) != 0) {
      co_return failure<orchfs::async::FileStat>(current_error(EIO));
    }
    co_return orchfs::async::Result<orchfs::async::FileStat>::success(
        to_stat(value));
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::FileSystemStat>>
  statfs(orchfs::async::InodeNumber inode) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    struct statfs value {};
    if (fd < 0) {
      co_return failure<orchfs::async::FileSystemStat>(EBADF);
    }
    if (orchfs_fstatfs(fd, &value) != 0) {
      co_return failure<orchfs::async::FileSystemStat>(current_error(EIO));
    }
    co_return orchfs::async::Result<orchfs::async::FileSystemStat>::success({
        .type = static_cast<std::uint64_t>(value.f_type),
        .block_size = static_cast<std::uint64_t>(value.f_bsize),
        .blocks = static_cast<std::uint64_t>(value.f_blocks),
        .blocks_free = static_cast<std::uint64_t>(value.f_bfree),
        .blocks_available = static_cast<std::uint64_t>(value.f_bavail),
        .files = static_cast<std::uint64_t>(value.f_files),
        .files_free = static_cast<std::uint64_t>(value.f_ffree),
        .name_length = static_cast<std::uint64_t>(value.f_namelen),
        .fragment_size = static_cast<std::uint64_t>(value.f_frsize),
        .flags = static_cast<std::uint64_t>(value.f_flags),
    });
  }

  orchfs::async::Task<orchfs::async::Result<std::uint64_t>> seek(
      orchfs::async::InodeNumber inode, std::uint64_t current_offset,
      std::int64_t offset, int whence) override {
    record_inode_owner_call(inode);
    std::uint64_t base = 0;
    if (whence == SEEK_CUR) {
      base = current_offset;
    } else if (whence == SEEK_END) {
      std::lock_guard lock(mutex_);
      const int fd = find_fd_locked(inode);
      struct stat value {};
      if (fd < 0) {
        co_return failure<std::uint64_t>(EBADF);
      }
      if (orchfs_fstat(fd, &value) != 0) {
        co_return failure<std::uint64_t>(current_error(EIO));
      }
      base = static_cast<std::uint64_t>(value.st_size);
    } else if (whence != SEEK_SET) {
      co_return failure<std::uint64_t>(EINVAL);
    }
    if (offset < 0) {
      const auto amount = static_cast<std::uint64_t>(-(offset + 1)) + 1;
      if (amount > base) {
        co_return failure<std::uint64_t>(EINVAL);
      }
      co_return orchfs::async::Result<std::uint64_t>::success(base - amount);
    }
    if (static_cast<std::uint64_t>(offset) >
        std::numeric_limits<std::uint64_t>::max() - base) {
      co_return failure<std::uint64_t>(EOVERFLOW);
    }
    co_return orchfs::async::Result<std::uint64_t>::success(
        base + static_cast<std::uint64_t>(offset));
  }

  orchfs::async::Task<orchfs::async::Result<void>> truncate(
      std::string path, std::uint64_t size) override {
    if (orchfs_truncate(path.c_str(), static_cast<std::size_t>(size)) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> truncate(
      orchfs::async::InodeNumber inode, std::uint64_t size) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    if (fd < 0) {
      co_return failure_void(EBADF);
    }
    if (orchfs_ftruncate(fd, static_cast<std::size_t>(size)) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> sync(
      orchfs::async::InodeNumber inode) override {
    record_inode_owner_call(inode);
    std::lock_guard lock(mutex_);
    const int fd = find_fd_locked(inode);
    if (fd < 0) {
      co_return failure_void(EBADF);
    }
    if (orchfs_fsync(fd) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> make_directory(
      std::string path, std::uint32_t mode) override {
    if (orchfs_mkdir(path.c_str(), static_cast<std::uint16_t>(mode)) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> remove_directory(
      std::string path) override {
    if (orchfs_rmdir(path.c_str()) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> unlink(
      std::string path) override {
    if (orchfs_unlink(path.c_str()) != 0) {
      co_return failure_void(current_error(EIO));
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<void>> rename(
      std::string old_path, std::string new_path) override {
    if (orchfs_rename(old_path.c_str(), new_path.c_str()) != 0) {
      co_return failure_void(current_error(EIO));
    }
    std::lock_guard lock(mutex_);
    for (auto& [inode, path] : paths_) {
      if (path == old_path) {
        path = new_path;
      }
    }
    co_return orchfs::async::Result<void>::success();
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::OpenedNode>>
  open_directory(std::string path) override {
    auto result = open_impl(-1, std::move(path), O_RDONLY, 0);
    if (!result) {
      co_return result;
    }
    if (result.value().type != orchfs::async::NodeType::directory) {
      co_return failure<orchfs::async::OpenedNode>(ENOTDIR);
    }
    co_return result;
  }

  orchfs::async::Task<orchfs::async::Result<orchfs::async::OpenedNode>>
  open_directory(orchfs::async::InodeNumber inode) override {
    record_namespace_handle_call();
    std::lock_guard lock(mutex_);
    const int source = find_fd_locked(inode);
    if (source < 0) {
      co_return failure<orchfs::async::OpenedNode>(EBADF);
    }
    struct stat value {};
    if (orchfs_fstat(source, &value) != 0 || !S_ISDIR(value.st_mode)) {
      co_return failure<orchfs::async::OpenedNode>(ENOTDIR);
    }
    const int duplicate = ::dup(source);
    if (duplicate < 0) {
      co_return failure<orchfs::async::OpenedNode>(current_error(EIO));
    }
    remember_file_handle(duplicate, paths_[inode]);
    descriptors_[inode].push_back(duplicate);
    co_return orchfs::async::Result<orchfs::async::OpenedNode>::success({
        .inode = inode,
        .type = orchfs::async::NodeType::directory,
    });
  }

  orchfs::async::Task<
      orchfs::async::Result<orchfs::async::DirectoryReadResult>>
  read_directory(orchfs::async::InodeNumber inode, std::uint64_t cursor,
                 std::span<orchfs::async::DirEntry> entries) override {
    record_inode_owner_call(inode);
    if (cursor < 512 || cursor % 256 != 0) {
      co_return failure<orchfs::async::DirectoryReadResult>(EINVAL);
    }
    std::string path;
    {
      std::lock_guard lock(mutex_);
      const auto found = paths_.find(inode);
      if (found == paths_.end()) {
        co_return failure<orchfs::async::DirectoryReadResult>(EBADF);
      }
      path = found->second;
    }
    DIR* directory = ::opendir(host_path(path.c_str()).c_str());
    if (directory == nullptr) {
      co_return failure<orchfs::async::DirectoryReadResult>(current_error(EIO));
    }
    const std::uint64_t skip = cursor / 256 - 2;
    std::uint64_t logical = 0;
    std::size_t count = 0;
    while (dirent* entry = ::readdir(directory)) {
      if (std::strcmp(entry->d_name, ".") == 0 ||
          std::strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      if (logical++ < skip) {
        continue;
      }
      if (count == entries.size()) {
        break;
      }
      entries[count++] = orchfs::async::DirEntry{
          .inode = static_cast<std::uint64_t>(entry->d_ino),
          .offset = static_cast<std::int64_t>(512 + logical * 256),
          .type = entry->d_type,
          .name = entry->d_name,
      };
    }
    (void)::closedir(directory);
    co_return orchfs::async::Result<
        orchfs::async::DirectoryReadResult>::success({
        .count = count,
        .next_cursor = 512 + logical * 256,
    });
  }

  orchfs::async::Task<orchfs::async::Result<void>> close_directory(
      orchfs::async::OpenedNode node) override {
    record_lifecycle_call();
    co_return close_impl(node.inode);
  }

 private:
  template <typename T>
  static orchfs::async::Result<T> failure(int error) {
    return orchfs::async::Result<T>::failure(
        std::error_code(error, std::generic_category()));
  }

  static orchfs::async::Result<void> failure_void(int error) {
    return orchfs::async::Result<void>::failure(
        std::error_code(error, std::generic_category()));
  }

  static int current_error(int fallback) noexcept {
    return errno != 0 ? errno : fallback;
  }

  static orchfs::async::NodeType type_for(mode_t mode) noexcept {
    if (S_ISDIR(mode)) {
      return orchfs::async::NodeType::directory;
    }
    if (S_ISREG(mode)) {
      return orchfs::async::NodeType::regular;
    }
    return orchfs::async::NodeType::unknown;
  }

  static orchfs::async::FileStat to_stat(const struct stat& value) noexcept {
    return {
        .device = value.st_dev,
        .inode = value.st_ino,
        .mode = value.st_mode,
        .link_count = value.st_nlink,
        .uid = value.st_uid,
        .gid = value.st_gid,
        .rdev = value.st_rdev,
        .size = value.st_size,
        .block_size = value.st_blksize,
        .blocks = value.st_blocks,
        .atime_seconds = value.st_atim.tv_sec,
        .atime_nanoseconds = value.st_atim.tv_nsec,
        .mtime_seconds = value.st_mtim.tv_sec,
        .mtime_nanoseconds = value.st_mtim.tv_nsec,
        .ctime_seconds = value.st_ctim.tv_sec,
        .ctime_nanoseconds = value.st_ctim.tv_nsec,
    };
  }

  orchfs::async::Result<orchfs::async::OpenedNode> open_impl(
      orchfs::async::InodeNumber directory, std::string path, int flags,
      std::uint32_t mode) {
    std::lock_guard lock(mutex_);
    int fd = -1;
    std::string full_path = path;
    if (directory >= 0 && !path.empty() && path.front() != '/') {
      const int directory_fd = find_fd_locked(directory);
      if (directory_fd < 0) {
        return failure<orchfs::async::OpenedNode>(EBADF);
      }
      fd = orchfs_openat(directory_fd, path.c_str(), flags, mode);
      const auto parent = paths_.find(directory);
      if (parent != paths_.end()) {
        full_path = parent->second;
        if (!full_path.empty() && full_path.back() != '/') {
          full_path.push_back('/');
        }
        full_path += path;
      }
    } else {
      fd = orchfs_open(path.c_str(), flags, mode);
    }
    if (fd < 0) {
      return failure<orchfs::async::OpenedNode>(current_error(ENOENT));
    }
    struct stat value {};
    if (orchfs_fstat(fd, &value) != 0) {
      const int error = current_error(EIO);
      (void)orchfs_close(fd);
      return failure<orchfs::async::OpenedNode>(error);
    }
    if ((flags & O_TRUNC) != 0 && S_ISREG(value.st_mode) &&
        orchfs_ftruncate(fd, 0) != 0) {
      const int error = current_error(EIO);
      (void)orchfs_close(fd);
      return failure<orchfs::async::OpenedNode>(error);
    }
    const auto type = type_for(value.st_mode);
    if (type == orchfs::async::NodeType::unknown) {
      (void)orchfs_close(fd);
      return failure<orchfs::async::OpenedNode>(ENOTSUP);
    }
    descriptors_[static_cast<std::int64_t>(value.st_ino)].push_back(fd);
    paths_.insert_or_assign(static_cast<std::int64_t>(value.st_ino),
                            std::move(full_path));
    return orchfs::async::Result<orchfs::async::OpenedNode>::success({
        .inode = static_cast<std::int64_t>(value.st_ino),
        .type = type,
    });
  }

  orchfs::async::Result<void> close_impl(
      orchfs::async::InodeNumber inode) {
    std::lock_guard lock(mutex_);
    const auto found = descriptors_.find(inode);
    if (found == descriptors_.end() || found->second.empty()) {
      return failure_void(EBADF);
    }
    const int fd = found->second.back();
    if (orchfs_close(fd) != 0) {
      return failure_void(current_error(EIO));
    }
    found->second.pop_back();
    if (found->second.empty()) {
      descriptors_.erase(found);
    }
    return orchfs::async::Result<void>::success();
  }

  int find_fd_locked(orchfs::async::InodeNumber inode) const noexcept {
    const auto found = descriptors_.find(inode);
    return found == descriptors_.end() || found->second.empty()
               ? -1
               : found->second.back();
  }

  std::mutex mutex_;
  std::unordered_map<orchfs::async::InodeNumber, std::vector<int>> descriptors_;
  std::unordered_map<orchfs::async::InodeNumber, std::string> paths_;
};

std::vector<unsigned> benchmark_cpu_set(const char* variable) {
  const char* value = std::getenv(variable);
  if (value == nullptr || *value == '\0') {
    return {};
  }

  std::vector<unsigned> cpus;
  const char* cursor = value;
  while (*cursor != '\0') {
    errno = 0;
    char* end = nullptr;
    const unsigned long cpu = std::strtoul(cursor, &end, 10);
    require(end != cursor && errno == 0 && cpu < CPU_SETSIZE,
            std::string("invalid CPU list in ") + variable);
    require(std::find(cpus.begin(), cpus.end(), static_cast<unsigned>(cpu)) ==
                cpus.end(),
            std::string("duplicate CPU in ") + variable);
    cpus.push_back(static_cast<unsigned>(cpu));
    if (*end == '\0') {
      break;
    }
    require(*end == ',' && end[1] != '\0',
            std::string("invalid CPU list in ") + variable);
    cursor = end + 1;
  }
  require(cpus.size() >= benchmark_stream_count,
          std::string("too few CPUs in ") + variable);
  return cpus;
}

std::size_t benchmark_spin_count() {
  const char* value = std::getenv("ORCHFS_ASYNC_BENCHMARK_SPIN");
  if (value == nullptr || *value == '\0') {
    return 256;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long count = std::strtoull(value, &end, 10);
  require(end != value && *end == '\0' && errno == 0 &&
              count <= std::numeric_limits<std::size_t>::max(),
          "invalid ORCHFS_ASYNC_BENCHMARK_SPIN");
  return static_cast<std::size_t>(count);
}

std::size_t benchmark_coroutine_count() {
  const char* value = std::getenv("ORCHFS_ASYNC_BENCHMARK_COROUTINES");
  if (value == nullptr || *value == '\0') {
    return 64;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long count = std::strtoull(value, &end, 10);
  require(end != value && *end == '\0' && errno == 0 && count != 0 &&
              count <= benchmark_stream_count * benchmark_timed_operations &&
              count % benchmark_stream_count == 0 &&
              (benchmark_stream_count * benchmark_timed_operations) % count ==
                  0,
          "invalid ORCHFS_ASYNC_BENCHMARK_COROUTINES");
  return static_cast<std::size_t>(count);
}

orchfs::async::Task<orchfs::async::Result<std::uint64_t>>
benchmark_write_stream(orchfs::async::File& file,
                       std::span<const std::byte> buffer,
                       std::size_t operations, std::uint64_t first_offset,
                       std::uint64_t offset_stride = 0,
                       bool sync_after = true,
                       std::vector<std::chrono::nanoseconds>* latencies =
                           nullptr) {
  if (offset_stride == 0) {
    offset_stride = buffer.size();
  }
  std::uint64_t completed = 0;
  for (std::size_t operation = 0; operation < operations; ++operation) {
    const auto offset = first_offset + operation * offset_stride;
    const auto started = std::chrono::steady_clock::now();
    auto written = co_await file.write_at(offset, buffer);
    if (latencies != nullptr) {
      latencies->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - started));
    }
    if (!written) {
      co_return orchfs::async::Result<std::uint64_t>::failure(written.error());
    }
    if (written.value() != buffer.size()) {
      co_return orchfs::async::Result<std::uint64_t>::failure(
          std::make_error_code(std::errc::io_error));
    }
    completed += written.value();
  }

  if (sync_after) {
    auto synced = co_await file.sync();
    if (!synced) {
      co_return orchfs::async::Result<std::uint64_t>::failure(synced.error());
    }
  }
  co_return orchfs::async::Result<std::uint64_t>::success(completed);
}

orchfs::async::Task<orchfs::async::Result<std::uint64_t>>
benchmark_read_stream(orchfs::async::File& file, std::span<std::byte> buffer,
                      std::size_t operations, std::uint64_t first_offset,
                      std::uint64_t offset_stride = 0,
                      std::vector<std::chrono::nanoseconds>* latencies =
                          nullptr) {
  if (offset_stride == 0) {
    offset_stride = buffer.size();
  }
  std::uint64_t completed = 0;
  for (std::size_t operation = 0; operation < operations; ++operation) {
    const auto offset = first_offset + operation * offset_stride;
    const auto started = std::chrono::steady_clock::now();
    auto read = co_await file.read_at(offset, buffer);
    if (latencies != nullptr) {
      latencies->push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - started));
    }
    if (!read) {
      co_return orchfs::async::Result<std::uint64_t>::failure(read.error());
    }
    if (read.value() != buffer.size()) {
      co_return orchfs::async::Result<std::uint64_t>::failure(
          std::make_error_code(std::errc::io_error));
    }
    completed += read.value();
  }
  co_return orchfs::async::Result<std::uint64_t>::success(completed);
}

struct BenchmarkSample {
  std::uint64_t bytes{};
  std::chrono::duration<double> elapsed{};
};

template <typename MakeTask>
BenchmarkSample run_benchmark_phase(orchfs::async::Runtime& runtime,
                                    std::size_t coroutine_count,
                                    MakeTask make_task) {
  std::vector<orchfs::async::JoinHandle<
      orchfs::async::Result<std::uint64_t>>>
      handles;
  handles.reserve(coroutine_count);

  const auto start = std::chrono::steady_clock::now();
  for (std::size_t coroutine = 0; coroutine < coroutine_count; ++coroutine) {
    auto submitted = runtime.submit(make_task(coroutine));
    if (!submitted) {
      throw std::system_error(submitted.error());
    }
    handles.push_back(std::move(submitted).value());
  }

  std::uint64_t bytes = 0;
  for (auto& handle : handles) {
    auto joined = std::move(handle).join();
    if (!joined) {
      throw std::system_error(joined.error());
    }
    auto result = std::move(joined).value();
    if (!result) {
      throw std::system_error(result.error());
    }
    bytes += result.value();
  }
  return {bytes, std::chrono::steady_clock::now() - start};
}

void print_benchmark_sample(std::string_view phase,
                            const BenchmarkSample& sample,
                            std::size_t coroutine_count,
                            const std::vector<std::vector<
                                std::chrono::nanoseconds>>& latencies) {
  const double seconds = sample.elapsed.count();
  const double mebibytes = static_cast<double>(sample.bytes) / (1024.0 * 1024.0);
  const double operations =
      static_cast<double>(sample.bytes) / benchmark_block_size;
  std::vector<std::chrono::nanoseconds> flattened;
  for (const auto& samples : latencies) {
    flattened.insert(flattened.end(), samples.begin(), samples.end());
  }
  double p99_microseconds = 0.0;
  if (!flattened.empty()) {
    const std::size_t percentile =
        (flattened.size() * 99 + 99) / 100 - 1;
    std::nth_element(flattened.begin(), flattened.begin() + percentile,
                     flattened.end());
    p99_microseconds =
        std::chrono::duration<double, std::micro>(flattened[percentile])
            .count();
  }
  std::printf(
      "orchfs_layered_benchmark layer=client_shm_rpc_kfs_e2e "
      "phase=%.*s block_size=%zu streams=%zu qd=%zu coroutines=%zu "
      "operations_per_stream=%zu bytes=%llu seconds=%.6f MiB_per_s=%.2f "
      "IOPS=%.2f p99_us=%.2f latency_samples=%zu\n",
      static_cast<int>(phase.size()), phase.data(), benchmark_block_size,
      benchmark_stream_count, coroutine_count, coroutine_count,
      benchmark_timed_operations,
      static_cast<unsigned long long>(sample.bytes), seconds,
      mebibytes / seconds, operations / seconds, p99_microseconds,
      flattened.size());
}

struct RawReply {
  orchfs::async::IpcDescriptor descriptor;
  std::vector<std::byte> payload;
};

std::vector<std::byte> make_raw_request(
    orchfs::async::RpcRequest request, std::string_view path1 = {},
    std::string_view path2 = {}, std::span<const std::byte> data = {}) {
  request.schema_version = orchfs::async::kRpcSchemaVersion;
  request.path1_length = static_cast<std::uint32_t>(path1.size());
  request.path2_length = static_cast<std::uint32_t>(path2.size());
  request.data_length = static_cast<std::uint32_t>(data.size());
  std::vector<std::byte> payload(sizeof(request) + path1.size() + path2.size() +
                                 data.size());
  std::byte* cursor = payload.data();
  std::memcpy(cursor, &request, sizeof(request));
  cursor += sizeof(request);
  if (!path1.empty()) {
    std::memcpy(cursor, path1.data(), path1.size());
  }
  cursor += path1.size();
  if (!path2.empty()) {
    std::memcpy(cursor, path2.data(), path2.size());
  }
  cursor += path2.size();
  if (!data.empty()) {
    std::memcpy(cursor, data.data(), data.size());
  }
  return payload;
}

void raw_submit(orchfs::async::ClientTransport& transport, std::uint32_t lane,
                orchfs::async::Opcode opcode,
                std::vector<std::byte> payload) {
  orchfs::async::IpcDescriptor request;
  request.opcode = opcode;
  request.flags = orchfs::async::DescriptorFlag::request;
  if (!payload.empty()) {
    request.flags |= orchfs::async::DescriptorFlag::has_payload;
  }
  const auto error = transport.try_submit(lane, request, payload);
  if (error) {
    throw std::system_error(error);
  }
}

void raw_submit_retry(orchfs::async::ClientTransport& transport,
                      std::uint32_t lane, orchfs::async::Opcode opcode,
                      const std::vector<std::byte>& payload) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  for (;;) {
    orchfs::async::IpcDescriptor request;
    request.opcode = opcode;
    request.flags = orchfs::async::DescriptorFlag::request;
    if (!payload.empty()) {
      request.flags |= orchfs::async::DescriptorFlag::has_payload;
    }
    const auto error = transport.try_submit(lane, request, payload);
    if (!error) {
      return;
    }
    if (error != orchfs::async::make_error_code(
                     orchfs::async::TransportErrc::would_block)) {
      throw std::system_error(error);
    }
    require(std::chrono::steady_clock::now() < deadline,
            "raw RPC submission timeout");
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
}

RawReply raw_receive_eventually(orchfs::async::ClientTransport& transport,
                                std::uint32_t lane) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  for (;;) {
    RawReply reply;
    reply.payload.resize(4096);
    std::size_t payload_size = 0;
    auto error = transport.try_receive_completion(
        lane, reply.descriptor, reply.payload, payload_size);
    if (!error) {
      reply.payload.resize(payload_size);
      return reply;
    }
    if (error != orchfs::async::make_error_code(
                     orchfs::async::TransportErrc::would_block)) {
      throw std::system_error(error);
    }
    require(std::chrono::steady_clock::now() < deadline,
            "raw RPC completion timeout");
    pollfd completion_fd{transport.completion_event_fd(lane), POLLIN, 0};
    int poll_result;
    do {
      poll_result = ::poll(&completion_fd, 1, 100);
    } while (poll_result < 0 && errno == EINTR);
    require(poll_result >= 0, "raw RPC completion poll failed");
    if (poll_result == 1 && (completion_fd.revents & POLLIN) != 0) {
      std::uint64_t notifications = 0;
      error =
          transport.drain_completion_notifications(lane, notifications);
      if (error) {
        throw std::system_error(error);
      }
    }
  }
}

void wait_for_completion_notifications(
    orchfs::async::ClientTransport& transport, std::uint32_t lane,
    std::uint64_t expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  std::uint64_t observed = 0;
  while (observed < expected) {
    require(std::chrono::steady_clock::now() < deadline,
            "completion ring did not fill");
    pollfd completion_fd{transport.completion_event_fd(lane), POLLIN, 0};
    int poll_result;
    do {
      poll_result = ::poll(&completion_fd, 1, 100);
    } while (poll_result < 0 && errno == EINTR);
    require(poll_result >= 0, "completion ring poll failed");
    if (poll_result == 0 || (completion_fd.revents & POLLIN) == 0) {
      continue;
    }
    std::uint64_t notifications = 0;
    const auto error =
        transport.drain_completion_notifications(lane, notifications);
    if (error) {
      throw std::system_error(error);
    }
    observed += notifications;
  }
}

RawReply raw_rpc(orchfs::async::ClientTransport& transport,
                 orchfs::async::Opcode opcode,
                 std::vector<std::byte> payload, std::uint32_t lane = 0) {
  raw_submit(transport, lane, opcode, std::move(payload));
  // eventfd notifications are readiness hints rather than a one-to-one queue:
  // a prior completion can leave a coalesced notification behind after its CQ
  // entry has already been consumed. Retry the ring until the requested reply
  // is actually visible.
  return raw_receive_eventually(transport, lane);
}

orchfs::async::ClientTransport connect_raw(
    const std::string& endpoint, orchfs::async::TransportConfig config) {
  std::error_code error;
  auto transport =
      orchfs::async::ClientTransport::connect(endpoint, config, error);
  if (error) {
    throw std::system_error(error);
  }
  require(static_cast<bool>(transport), "raw transport connect failed");
  return transport;
}

orchfs::async::ClientTransport connect_raw(const std::string& endpoint) {
  return connect_raw(
      endpoint,
      {.lane_count = 2, .ring_capacity = 8, .data_slot_size = 4096});
}

int connect_without_hello(const std::string& endpoint) {
  const int fd = ::socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  if (fd < 0) {
    throw std::system_error(errno, std::generic_category());
  }
  sockaddr_un address{};
  address.sun_family = AF_UNIX;
  require(endpoint.size() < sizeof(address.sun_path),
          "control endpoint is too long");
  std::memcpy(address.sun_path, endpoint.data(), endpoint.size());
  address.sun_path[endpoint.size()] = '\0';
  const auto length = static_cast<socklen_t>(
      offsetof(sockaddr_un, sun_path) + endpoint.size() + 1U);
  if (::connect(fd, reinterpret_cast<sockaddr*>(&address), length) != 0) {
    const int error = errno;
    ::close(fd);
    throw std::system_error(error, std::generic_category());
  }
  return fd;
}

orchfs::async::RemoteHandle raw_open(
    orchfs::async::ClientTransport& transport, std::string_view path,
    int flags = O_CREAT | O_RDWR, std::uint32_t lane = 0) {
  orchfs::async::RpcRequest request;
  request.open_flags = flags;
  request.mode = 0644;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::open,
                             make_raw_request(request, path), lane);
  require(reply.descriptor.status == 0, "raw open failed");
  return reply.descriptor.result_length;
}

void raw_close(orchfs::async::ClientTransport& transport,
               orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::close,
                             make_raw_request(request));
  require(reply.descriptor.status == 0, "raw close failed");
}

orchfs::async::RpcFileStat raw_stat(
    orchfs::async::ClientTransport& transport,
    orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto reply = raw_rpc(transport, orchfs::async::Opcode::stat_handle,
                             make_raw_request(request));
  require(reply.descriptor.status == 0 &&
              reply.payload.size() == sizeof(orchfs::async::RpcFileStat),
          "raw fstat failed");
  orchfs::async::RpcFileStat value;
  std::memcpy(&value, reply.payload.data(), sizeof(value));
  return value;
}

void submit_stat_burst(orchfs::async::ClientTransport& transport,
                       orchfs::async::RemoteHandle handle) {
  orchfs::async::RpcRequest request;
  request.handle = handle;
  const auto payload = make_raw_request(request);
  for (int index = 0; index < 3; ++index) {
    raw_submit_retry(transport, 0, orchfs::async::Opcode::stat_handle,
                     payload);
  }
  wait_for_completion_notifications(transport, 0, 2);
  // The first two replies now occupy the entire CQ. Give the server I/O loop
  // time to exercise the would_block retry path for the third non-empty reply.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
}

void exercise_completion_backpressure(const std::string& endpoint) {
  auto transport = connect_raw(
      endpoint,
      {.lane_count = 1, .ring_capacity = 2, .data_slot_size = 4096});
  const auto handle = raw_open(transport, "/completion-backpressure");
  submit_stat_burst(transport, handle);
  for (int index = 0; index < 3; ++index) {
    const auto reply = raw_receive_eventually(transport, 0);
    require(reply.descriptor.status == 0 &&
                reply.payload.size() == sizeof(orchfs::async::RpcFileStat),
            "CQ retry lost a completion payload");
  }
  raw_close(transport, handle);
}

orchfs::async::ClientTransport make_saturated_session(
    const std::string& endpoint) {
  auto transport = connect_raw(
      endpoint,
      {.lane_count = 1, .ring_capacity = 2, .data_slot_size = 4096});
  const auto handle = raw_open(transport, "/saturated-on-server-stop");
  submit_stat_burst(transport, handle);
  return transport;
}

void exercise_close_capture_race(
    orchfs::async::ClientTransport& transport) {
  for (int iteration = 0; iteration < 64; ++iteration) {
    const std::string original_path =
        "/capture-race-original-" + std::to_string(iteration);
    const auto original = raw_open(transport, original_path);
    const auto original_stat = raw_stat(transport, original);

    orchfs::async::RpcRequest close_request;
    close_request.handle = original;
    orchfs::async::RpcRequest stat_request;
    stat_request.handle = original;
    // Different lanes let both Runtime roots capture/schedule independently.
    // If stat captures first but close wins lifecycle-write, stat must return
    // EBADF rather than touching a subsequently reused legacy descriptor.
    raw_submit(transport, 0, orchfs::async::Opcode::stat_handle,
               make_raw_request(stat_request));
    raw_submit(transport, 1, orchfs::async::Opcode::close,
               make_raw_request(close_request));
    const auto close_reply = raw_receive_eventually(transport, 1);
    require(close_reply.descriptor.status == 0,
            "capture-race close failed");

    const std::string replacement_path =
        "/capture-race-replacement-" + std::to_string(iteration);
    const auto replacement =
        raw_open(transport, replacement_path, O_CREAT | O_RDWR, 1);
    const auto stat_reply = raw_receive_eventually(transport, 0);
    if (stat_reply.descriptor.status == 0) {
      require(stat_reply.payload.size() ==
                  sizeof(orchfs::async::RpcFileStat),
              "capture-race stat payload malformed");
      orchfs::async::RpcFileStat raced_stat;
      std::memcpy(&raced_stat, stat_reply.payload.data(), sizeof(raced_stat));
      require(raced_stat.inode == original_stat.inode,
              "captured handle observed a reused legacy fd");
    } else {
      require(stat_reply.descriptor.status == -EBADF,
              "capture-race stat returned an unexpected error");
    }
    raw_close(transport, replacement);
  }
}

void exercise_raw_openat_and_disconnect(const std::string& endpoint) {
  auto transport = connect_raw(endpoint);
  const auto file_handle = raw_open(transport, "/raw-file");

  orchfs::async::RpcRequest invalid_io;
  invalid_io.handle = file_handle;
  invalid_io.offset = -1;
  invalid_io.length = 1;
  auto reply = raw_rpc(transport, orchfs::async::Opcode::read_at,
                       make_raw_request(invalid_io));
  require(reply.descriptor.status == -EINVAL,
          "negative positioned read reached the legacy core");
  const std::array<std::byte, 1> invalid_write{std::byte{0x5a}};
  reply = raw_rpc(transport, orchfs::async::Opcode::write_at,
                  make_raw_request(invalid_io, {}, {}, invalid_write));
  require(reply.descriptor.status == -EINVAL,
          "negative positioned write reached the legacy core");
  invalid_io.offset = std::numeric_limits<std::int64_t>::max();
  invalid_io.length = 2;
  reply = raw_rpc(transport, orchfs::async::Opcode::read_at,
                  make_raw_request(invalid_io));
  require(reply.descriptor.status == -EOVERFLOW,
          "overflowing positioned I/O range was accepted");

  orchfs::async::RpcRequest open_at;
  open_at.handle = std::numeric_limits<orchfs::async::RemoteHandle>::max();
  open_at.open_flags = O_CREAT | O_RDWR;
  open_at.mode = 0644;
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "relative-invalid"));
  require(reply.descriptor.status == -EBADF,
          "relative open_at accepted an invalid handle");

  open_at.handle = file_handle;
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "relative-nondirectory"));
  require(reply.descriptor.status == -ENOTDIR,
          "relative open_at accepted a non-directory handle");

  open_at.handle = std::numeric_limits<orchfs::async::RemoteHandle>::max();
  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, "/absolute-openat"));
  require(reply.descriptor.status == 0,
          "absolute open_at incorrectly validated dirfd");
  raw_close(transport, reply.descriptor.result_length);

  reply = raw_rpc(transport, orchfs::async::Opcode::open_at,
                  make_raw_request(open_at, ""));
  require(reply.descriptor.status == -ENOENT,
          "empty open_at path reached the legacy core");

  orchfs::async::RpcRequest missing;
  missing.open_flags = O_RDONLY;
  reply = raw_rpc(transport, orchfs::async::Opcode::open,
                  make_raw_request(missing, "/definitely-missing"));
  require(reply.descriptor.status == -ENOENT,
          "missing open did not return ENOENT");

  raw_close(transport, file_handle);
  exercise_close_capture_race(transport);
  (void)raw_open(transport, "/abandoned-on-disconnect");
  // Transport destruction is intentional: the server must run the same
  // once-only cleanup used by explicit shutdown.
}

struct SharedLibraryCloser {
  void operator()(void* library) const noexcept {
    if (library != nullptr) {
      (void)::dlclose(library);
    }
  }
};

template <typename Function>
Function wrapper_symbol(void* library, const char* name) {
  (void)::dlerror();
  void* address = ::dlsym(library, name);
  const char* error = ::dlerror();
  require(error == nullptr && address != nullptr,
          std::string("dlsym failed for ") + name +
              (error == nullptr ? "" : std::string(": ") + error));
  Function function{};
  static_assert(sizeof(function) == sizeof(address));
  std::memcpy(&function, &address, sizeof(function));
  return function;
}

struct RemoveTree {
  std::filesystem::path path;

  ~RemoveTree() {
    std::error_code ignored;
    std::filesystem::remove_all(path, ignored);
  }
};

struct RestoreWorkingDirectory {
  int fd{-1};

  ~RestoreWorkingDirectory() {
    if (fd >= 0) {
      (void)::fchdir(fd);
      (void)::close(fd);
    }
  }
};

int run_wrapper_client(const std::string& endpoint) {
  try {
    for (int retry = 0; retry < 200 && ::access(endpoint.c_str(), F_OK) != 0;
         ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    require(::access(endpoint.c_str(), F_OK) == 0,
            "wrapper client timed out waiting for server");
    require(::setenv("ORCHFS_ASYNC_ENDPOINT", endpoint.c_str(), 1) == 0,
            "setenv(ORCHFS_ASYNC_ENDPOINT) failed");
    require(::setenv("ORCHFS_CLIENT_WORKERS", "1", 1) == 0,
            "setenv(ORCHFS_CLIENT_WORKERS) failed");
    require(::setenv("ORCHFS_IPC_RING_CAPACITY", "8", 1) == 0,
            "setenv(ORCHFS_IPC_RING_CAPACITY) failed");
    require(::setenv("ORCHFS_IPC_DATA_SLOT_SIZE", "4096", 1) == 0,
            "setenv(ORCHFS_IPC_DATA_SLOT_SIZE) failed");

    void* loaded = ::dlopen(ORCHFS_WRAPPER_PATH, RTLD_NOW | RTLD_LOCAL);
    const char* load_error = loaded == nullptr ? ::dlerror() : nullptr;
    require(loaded != nullptr,
            std::string("dlopen wrapper failed") +
                (load_error == nullptr ? "" : std::string(": ") + load_error));
    std::unique_ptr<void, SharedLibraryCloser> library(loaded);

    using Open = int (*)(const char*, int, ...);
    using OpenAt = int (*)(int, const char*, int, ...);
    using Close = int (*)(int);
    using Mkdir = int (*)(const char*, mode_t);
    using Unlink = int (*)(const char*);
    using Rmdir = int (*)(const char*);
    using Rename = int (*)(const char*, const char*);
    const auto wrapped_open = wrapper_symbol<Open>(loaded, "open");
    const auto wrapped_openat = wrapper_symbol<OpenAt>(loaded, "openat");
    const auto wrapped_close = wrapper_symbol<Close>(loaded, "close");
    const auto wrapped_mkdir = wrapper_symbol<Mkdir>(loaded, "mkdir");
    const auto wrapped_unlink = wrapper_symbol<Unlink>(loaded, "unlink");
    const auto wrapped_rmdir = wrapper_symbol<Rmdir>(loaded, "rmdir");
    const auto wrapped_rename = wrapper_symbol<Rename>(loaded, "rename");

    require(wrapped_mkdir("\\/adapter-openat-directory", 0755) == 0,
            "wrapper mkdir fixture failed");
    const int directory =
        wrapped_open("\\/adapter-openat-directory", O_RDONLY | O_CLOEXEC);
    require(directory >= 1048576,
            "plain directory open did not return a virtual descriptor");
    const int child = wrapped_openat(
        directory, "child", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
    require(child >= 1048576,
            "openat rejected a directory opened without O_DIRECTORY");
    require(wrapped_close(child) == 0 && wrapped_close(directory) == 0,
            "wrapper directory descriptor close failed");
    struct stat child_stat {};
    require(::stat((root_path + "/adapter-openat-directory/child").c_str(),
                   &child_stat) == 0 &&
                S_ISREG(child_stat.st_mode),
            "wrapper openat created no file under the anchored directory");

    const int regular = wrapped_open("\\/adapter-openat-regular",
                                     O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
                                     0644);
    require(regular >= 1048576,
            "wrapper regular-file fixture open failed");
    errno = 0;
    const int rejected = wrapped_openat(
        regular, "child", O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
    const int rejected_error = errno;
    require(rejected == -1 && rejected_error == ENOTDIR,
            "regular virtual descriptor was accepted as an openat dirfd");
    require(wrapped_close(regular) == 0,
            "wrapper regular descriptor close failed");
    require(wrapped_unlink("\\/adapter-openat-directory/child") == 0 &&
                wrapped_rmdir("\\/adapter-openat-directory") == 0 &&
                wrapped_unlink("\\/adapter-openat-regular") == 0,
            "wrapper openat fixture cleanup failed");

    const auto host_directory =
        std::filesystem::temp_directory_path() /
        ("orchfs-wrapper-rename-" + std::to_string(::getpid()));
    std::error_code cleanup_error;
    std::filesystem::remove_all(host_directory, cleanup_error);
    require(std::filesystem::create_directory(host_directory),
            "host rename sandbox creation failed");
    RemoveTree remove_host_directory{host_directory};
    RestoreWorkingDirectory restore_cwd{
        ::open(".", O_RDONLY | O_DIRECTORY | O_CLOEXEC)};
    require(restore_cwd.fd >= 0, "saving cwd failed");
    require(::chdir(host_directory.c_str()) == 0,
            "entering host rename sandbox failed");
    constexpr char source[] = "host-only-rename-source";
    constexpr char target[] = "host-only-rename-target";
    const int host_file =
        ::open(source, O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC, 0644);
    require(host_file >= 0 && ::close(host_file) == 0,
            "host-only rename fixture creation failed");
    errno = 0;
    const int rename_result = wrapped_rename(source, target);
    const int rename_error = errno;
    require(rename_result == -1 && rename_error == ENOENT,
            "missing relative OrchFS rename was retried in the host namespace");
    require(::access(source, F_OK) == 0 && ::access(target, F_OK) != 0 &&
                errno == ENOENT,
            "failed OrchFS rename mutated the host namespace");
    require(::unlink(source) == 0, "host rename fixture cleanup failed");

    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "wrapper client failure: %s\n", error.what());
    return 1;
  }
}

int run_benchmark_client(const std::string& endpoint) {
  try {
    for (int retry = 0; retry < 200 && ::access(endpoint.c_str(), F_OK) != 0;
         ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    require(::access(endpoint.c_str(), F_OK) == 0,
            "benchmark client timed out waiting for server");

    orchfs::async::RuntimeOptions runtime_options;
    runtime_options.worker_count = benchmark_stream_count;
    runtime_options.cpu_set =
        benchmark_cpu_set("ORCHFS_ASYNC_BENCHMARK_CLIENT_CPUS");
    runtime_options.spin_before_park = benchmark_spin_count();
    auto created = orchfs::async::Runtime::create(std::move(runtime_options));
    require(static_cast<bool>(created), "benchmark Runtime::create failed");
    auto runtime = std::move(created).value();

    orchfs::async::ClientOptions client_options;
    client_options.endpoint = endpoint;
    client_options.ring_capacity = 64;
    client_options.data_slot_size =
        benchmark_block_size + sizeof(orchfs::async::RpcRequest);
    auto client = run(
        *runtime, orchfs::async::Client::connect(*runtime, client_options));

    std::vector<orchfs::async::File> files;
    std::vector<std::vector<std::byte>> buffers;
    std::vector<std::string> paths;
    files.reserve(benchmark_stream_count);
    buffers.reserve(benchmark_stream_count);
    paths.reserve(benchmark_stream_count);
    for (std::size_t stream = 0; stream < benchmark_stream_count; ++stream) {
      paths.push_back("/async-benchmark-" + std::to_string(::getpid()) + "-" +
                      std::to_string(stream));
      files.push_back(run(
          *runtime,
          client.open(paths.back(), O_CREAT | O_RDWR | O_TRUNC, 0644)));
      buffers.emplace_back(
          benchmark_block_size,
          static_cast<std::byte>(static_cast<unsigned char>(stream + 1)));
    }

    const auto warmup_write = run_benchmark_phase(
        *runtime, benchmark_stream_count, [&](std::size_t stream) {
          return benchmark_write_stream(files[stream], buffers[stream],
                                        benchmark_warmup_operations, 0);
        });
    require(warmup_write.bytes == benchmark_stream_count *
                                      benchmark_warmup_operations *
                                      benchmark_block_size,
            "benchmark write warmup was incomplete");
    const auto warmup_read = run_benchmark_phase(
        *runtime, benchmark_stream_count, [&](std::size_t stream) {
          return benchmark_read_stream(files[stream], buffers[stream],
                                       benchmark_warmup_operations, 0);
        });
    require(warmup_read.bytes == warmup_write.bytes,
            "benchmark read warmup was incomplete");

    const std::uint64_t timed_offset =
        benchmark_warmup_operations * benchmark_block_size;
    const std::uint64_t timed_end =
        timed_offset + benchmark_timed_operations * benchmark_block_size;
    // The legacy OrchFS core does not create sparse holes with pwrite: an
    // offset beyond the current end returns a short write. Preallocate outside
    // the sample so strided coroutine lanes measure steady-state data writes
    // with identical file extents on the host mock and real KFS/SPDK paths.
    for (auto& file : files) {
      run(*runtime, file.truncate(timed_end));
    }
    const char* phase_value =
        std::getenv("ORCHFS_ASYNC_BENCHMARK_PHASE");
    const std::string_view phase =
        phase_value == nullptr ? std::string_view("both") : phase_value;
    require(phase == "write" || phase == "read" || phase == "both",
            "ORCHFS_ASYNC_BENCHMARK_PHASE must be write, read, or both");
    const bool measure_write = phase != "read";
    const bool measure_read = phase != "write";
    const std::size_t coroutine_count = benchmark_coroutine_count();
    const std::size_t coroutines_per_stream =
        coroutine_count / benchmark_stream_count;
    const std::size_t operations_per_coroutine =
        benchmark_stream_count * benchmark_timed_operations /
        coroutine_count;
    const std::uint64_t coroutine_stride =
        coroutines_per_stream * benchmark_block_size;
    std::vector<std::vector<std::byte>> read_buffers(
        coroutine_count, std::vector<std::byte>(benchmark_block_size));
    std::vector<std::vector<std::chrono::nanoseconds>> write_latencies(
        coroutine_count);
    std::vector<std::vector<std::chrono::nanoseconds>> read_latencies(
        coroutine_count);
    for (std::size_t coroutine = 0; coroutine < coroutine_count;
         ++coroutine) {
      write_latencies[coroutine].reserve(operations_per_coroutine);
      read_latencies[coroutine].reserve(operations_per_coroutine);
    }

    // Allocate and populate the complete timed range outside the sample. The
    // measured write is a steady aligned overwrite in every benchmark layer,
    // so metadata allocation is not accidentally charged only to RPC E2E.
    (void)run_benchmark_phase(
        *runtime, coroutine_count, [&](std::size_t coroutine) {
          const std::size_t stream = coroutine % benchmark_stream_count;
          const std::size_t lane = coroutine / benchmark_stream_count;
          return benchmark_write_stream(files[stream], buffers[stream],
                                        operations_per_coroutine,
                                        timed_offset +
                                            lane * benchmark_block_size,
                                        coroutine_stride, false);
        });
    (void)run_benchmark_phase(
        *runtime, benchmark_stream_count, [&](std::size_t stream) {
          return benchmark_write_stream(files[stream], buffers[stream], 0, 0);
        });

    BenchmarkSample write_sample;
    if (measure_write) {
      write_sample = run_benchmark_phase(
          *runtime, coroutine_count, [&](std::size_t coroutine) {
            const std::size_t stream = coroutine % benchmark_stream_count;
            const std::size_t lane = coroutine / benchmark_stream_count;
            return benchmark_write_stream(files[stream], buffers[stream],
                                          operations_per_coroutine,
                                          timed_offset +
                                              lane * benchmark_block_size,
                                          coroutine_stride, false,
                                          &write_latencies[coroutine]);
          });
      const auto sync_sample = run_benchmark_phase(
          *runtime, benchmark_stream_count, [&](std::size_t stream) {
            return benchmark_write_stream(files[stream], buffers[stream], 0,
                                          0);
          });
      write_sample.elapsed += sync_sample.elapsed;
    }

    BenchmarkSample read_sample;
    if (measure_read) {
      read_sample = run_benchmark_phase(
          *runtime, coroutine_count, [&](std::size_t coroutine) {
            const std::size_t stream = coroutine % benchmark_stream_count;
            const std::size_t lane = coroutine / benchmark_stream_count;
            return benchmark_read_stream(
                files[stream], read_buffers[coroutine],
                operations_per_coroutine,
                timed_offset + lane * benchmark_block_size,
                coroutine_stride, &read_latencies[coroutine]);
          });
    }

    const std::uint64_t expected_bytes =
        benchmark_stream_count * benchmark_timed_operations *
        benchmark_block_size;
    if (measure_write) {
      require(write_sample.bytes == expected_bytes,
              "benchmark write phase was incomplete");
      print_benchmark_sample("write+sync", write_sample, coroutine_count,
                             write_latencies);
    }
    if (measure_read) {
      require(read_sample.bytes == expected_bytes,
              "benchmark read phase was incomplete");
      for (std::size_t coroutine = 0; coroutine < coroutine_count;
           ++coroutine) {
        const std::size_t stream = coroutine % benchmark_stream_count;
        const auto expected =
            static_cast<std::byte>(static_cast<unsigned char>(stream + 1));
        require(std::all_of(read_buffers[coroutine].begin(),
                            read_buffers[coroutine].end(),
                            [expected](std::byte value) {
                              return value == expected;
                            }),
                "benchmark read data mismatch");
      }
      print_benchmark_sample("read", read_sample, coroutine_count,
                             read_latencies);
    }

    for (std::size_t stream = 0; stream < benchmark_stream_count; ++stream) {
      run(*runtime, files[stream].close());
      run(*runtime, client.unlink(paths[stream]));
    }
    run(*runtime, client.shutdown());
    runtime->request_stop();
    require(static_cast<bool>(runtime->join()),
            "benchmark Runtime::join failed");
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "benchmark client failure: %s\n", error.what());
    return 1;
  }
}

int run_client(const std::string& endpoint) {
  try {
    for (int retry = 0; retry < 200 && ::access(endpoint.c_str(), F_OK) != 0;
         ++retry) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    exercise_raw_openat_and_disconnect(endpoint);

    orchfs::async::RuntimeOptions runtime_options;
    runtime_options.worker_count = 2;
    auto created = orchfs::async::Runtime::create(std::move(runtime_options));
    require(static_cast<bool>(created), "client Runtime::create failed");
    auto runtime = std::move(created).value();

    orchfs::async::ClientOptions client_options;
    client_options.endpoint = endpoint;
    client_options.ring_capacity = 8;
    client_options.data_slot_size = 4096;
    auto client = run(*runtime,
                      orchfs::async::Client::connect(*runtime, client_options));

    std::string oversized_path(client_options.data_slot_size, 'x');
    oversized_path.front() = '/';
    require(run_error(*runtime,
                      client.open(std::move(oversized_path), O_RDONLY)) ==
                std::make_error_code(std::errc::message_size),
            "oversized request did not return message_size");

    auto file = run(*runtime, client.open("/alpha", O_CREAT | O_RDWR, 0644));
    const std::array<std::byte, 11> message{
        std::byte{'h'}, std::byte{'e'}, std::byte{'l'}, std::byte{'l'},
        std::byte{'o'}, std::byte{' '}, std::byte{'o'}, std::byte{'r'},
        std::byte{'c'}, std::byte{'h'}, std::byte{'!'},
    };
    require(run(*runtime, file.write(message)) == message.size(),
            "short async write");
    require(run(*runtime, file.seek(0, SEEK_SET)) == 0, "seek failed");

    std::array<std::byte, message.size()> read_buffer{};
    require(run(*runtime, file.read(read_buffer)) == read_buffer.size(),
            "short async read");
    require(read_buffer == message, "read data mismatch");

    const std::array<std::byte, 5> patch{
        std::byte{'C'}, std::byte{'+'}, std::byte{'+'}, std::byte{'2'},
        std::byte{'0'},
    };
    require(run(*runtime, file.write_at(6, patch)) == patch.size(),
            "short positioned write");
    std::array<std::byte, patch.size()> patch_read{};
    require(run(*runtime, file.read_at(6, patch_read)) == patch_read.size(),
            "short positioned read");
    require(patch_read == patch, "positioned data mismatch");

    run(*runtime, file.truncate(0));
    require(run(*runtime, file.seek(0, SEEK_SET)) == 0,
            "seek before serialized writes failed");
    std::vector<std::byte> first_write(9000, std::byte{'A'});
    std::vector<std::byte> second_write(9000, std::byte{'B'});
    auto first_submitted = runtime->submit(file.write(first_write));
    auto second_submitted = runtime->submit(file.write(second_write));
    require(first_submitted && second_submitted,
            "concurrent implicit write submit failed");
    auto first_joined = std::move(first_submitted).value().join();
    auto second_joined = std::move(second_submitted).value().join();
    require(first_joined && second_joined,
            "concurrent implicit write join failed");
    auto first_result = std::move(first_joined).value();
    auto second_result = std::move(second_joined).value();
    require(first_result && first_result.value() == first_write.size() &&
                second_result && second_result.value() == second_write.size(),
            "concurrent implicit write failed");
    std::vector<std::byte> serialized(first_write.size() + second_write.size());
    require(run(*runtime, file.read_at(0, serialized)) == serialized.size(),
            "serialized write readback failed");
    const bool first_then_second =
        std::all_of(serialized.begin(),
                    serialized.begin() + static_cast<std::ptrdiff_t>(first_write.size()),
                    [](std::byte value) { return value == std::byte{'A'}; }) &&
        std::all_of(serialized.begin() +
                        static_cast<std::ptrdiff_t>(first_write.size()),
                    serialized.end(),
                    [](std::byte value) { return value == std::byte{'B'}; });
    const bool second_then_first =
        std::all_of(serialized.begin(),
                    serialized.begin() + static_cast<std::ptrdiff_t>(second_write.size()),
                    [](std::byte value) { return value == std::byte{'B'}; }) &&
        std::all_of(serialized.begin() +
                        static_cast<std::ptrdiff_t>(second_write.size()),
                    serialized.end(),
                    [](std::byte value) { return value == std::byte{'A'}; });
    require(first_then_second || second_then_first,
            "multi-chunk implicit writes interleaved");

    const auto file_stat = run(*runtime, file.stat());
    require(file_stat.size == static_cast<std::int64_t>(serialized.size()),
            "bad file size");
    constexpr std::int64_t large_offset = 3LL * 1024 * 1024 * 1024;
    require(run(*runtime, file.seek(large_offset, SEEK_SET)) ==
                static_cast<std::uint64_t>(large_offset),
            "64-bit seek was truncated by the RPC/legacy boundary");
    require(run(*runtime, file.seek(0, SEEK_CUR)) ==
                static_cast<std::uint64_t>(large_offset),
            "64-bit SEEK_CUR position mismatch");
    require(run(*runtime, file.seek(0, SEEK_END)) == serialized.size(),
            "SEEK_END(0) did not select the file size");
    run(*runtime, file.sync());
    run(*runtime, file.close());

    auto relative_file =
        run(*runtime, client.open("relative-old", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, relative_file.write(message)) == message.size(),
            "relative rename fixture write failed");
    run(*runtime, relative_file.close());
    constexpr std::size_t legacy_name_max = 230;
    const std::string maximum_name(legacy_name_max, 'm');
    const std::string oversized_name(legacy_name_max + 1, 'x');
    run(*runtime, client.rename("relative-old", maximum_name));
    require(run_error(*runtime,
                      client.rename(maximum_name, oversized_name)) ==
                std::errc::filename_too_long,
            "oversized rename basename reached the legacy core");
    run(*runtime, client.rename(maximum_name, "relative-new"));
    require(run(*runtime, client.stat("relative-new")).size ==
                static_cast<std::int64_t>(message.size()),
            "bare relative rename did not preserve the file");
    require(run_error(*runtime, client.stat("relative-old")) ==
                std::errc::no_such_file_or_directory,
            "bare relative rename left the old name visible");
    run(*runtime, client.unlink("relative-new"));

    run(*runtime, client.rename("/alpha", "/renamed"));
    auto deferred_stat = client.stat(std::string("/renamed"));
    require(run(*runtime, std::move(deferred_stat)).size ==
                static_cast<std::int64_t>(serialized.size()),
            "path copy/stat failed");

#ifdef O_DIRECTORY
    require(run_error(
                *runtime,
                client.open("/renamed", O_RDONLY | O_DIRECTORY)) ==
                std::errc::not_a_directory,
            "O_DIRECTORY accepted a regular file");
    require(run_error(
                *runtime,
                client.open("/directory-create-side-effect",
                            O_RDONLY | O_CREAT | O_DIRECTORY, 0755)) ==
                std::errc::invalid_argument,
            "O_CREAT|O_DIRECTORY was not rejected");
    require(run_error(*runtime,
                      client.stat("/directory-create-side-effect")) ==
                std::errc::no_such_file_or_directory,
            "failed O_CREAT|O_DIRECTORY left a regular file behind");
#endif

    run(*runtime, client.make_directory("/directory", 0755));
    auto directory = run(*runtime, client.open_directory("/"));
    std::array<orchfs::async::DirEntry, 32> entries;
    const auto count = run(*runtime, directory.next_batch(entries));
    bool saw_renamed = false;
    bool saw_directory = false;
    for (std::size_t index = 0; index < count; ++index) {
      saw_renamed |= entries[index].name == "renamed";
      saw_directory |= entries[index].name == "directory";
    }
    require(saw_renamed && saw_directory, "directory batch missed entries");
    run(*runtime, directory.close());

    run(*runtime, client.make_directory("/directory-anchor", 0755));
    auto directory_file = run(
        *runtime,
        client.open("/directory-anchor", O_RDONLY | O_DIRECTORY));
    run(*runtime,
        client.rename("/directory-anchor", "/directory-anchor-moved"));
    run(*runtime, client.make_directory("/directory-anchor", 0755));
    auto anchored_directory =
        run(*runtime, client.open_directory(directory_file));
    auto anchored_child = run(
        *runtime,
        client.open_at(anchored_directory, "child", O_CREAT | O_RDWR, 0644));
    run(*runtime, anchored_child.close());
    require(run(*runtime, client.stat("/directory-anchor-moved/child")).size ==
                0,
            "directory handle did not follow the already-open inode");
    require(run_error(*runtime, client.stat("/directory-anchor/child")) ==
                std::errc::no_such_file_or_directory,
            "directory handle was re-resolved through the recreated path");
    run(*runtime, anchored_directory.close());
    run(*runtime, directory_file.close());
    run(*runtime, client.unlink("/directory-anchor-moved/child"));
    run(*runtime, client.remove_directory("/directory-anchor-moved"));
    run(*runtime, client.remove_directory("/directory-anchor"));

    run(*runtime, client.unlink("/renamed"));
    run(*runtime, client.remove_directory("/directory"));

    auto close_retry =
        run(*runtime, client.open("/close-retry", O_CREAT | O_RDWR, 0644));
    auto close_error = run_error(*runtime, close_retry.close());
    require(close_error == std::errc::io_error,
            "injected close failure was not returned");
    require(run(*runtime, close_retry.stat()).size == 0,
            "failed close made the remote handle unusable");
    run(*runtime, close_retry.close());

    auto over_write =
        run(*runtime, client.open("/over-write", O_CREAT | O_RDWR, 0644));
    const std::array<std::byte, 1> one_byte{std::byte{'W'}};
    require(run_error(*runtime, over_write.write(one_byte)) ==
                std::errc::io_error,
            "write over-return escaped server validation");
    run(*runtime, over_write.close());

    auto over_read =
        run(*runtime, client.open("/over-read", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, over_read.write(one_byte)) == one_byte.size(),
            "over-read fixture write failed");
    require(run(*runtime, over_read.seek(0, SEEK_SET)) == 0,
            "over-read fixture seek failed");
    std::array<std::byte, 1> over_read_buffer{};
    require(run_error(*runtime, over_read.read(over_read_buffer)) ==
                std::errc::io_error,
            "read over-return escaped server validation");
    run(*runtime, over_read.close());

    auto truncate_file =
        run(*runtime, client.open("/truncate-file", O_CREAT | O_RDWR, 0644));
    require(run(*runtime, truncate_file.write(message)) == message.size(),
            "truncate fixture write failed");
    run(*runtime, truncate_file.truncate(7));
    require(run(*runtime, truncate_file.stat()).size == 7,
            "handle truncate failed");
    run(*runtime, truncate_file.close());
    run(*runtime, client.truncate("/truncate-file", 3));
    require(run(*runtime, client.stat("/truncate-file")).size == 3,
            "path truncate failed");
    auto truncate_open = run(
        *runtime,
        client.open("/truncate-file", O_RDWR | O_TRUNC, 0644));
    require(run(*runtime, truncate_open.stat()).size == 0,
            "O_TRUNC was not applied under the inode gate");
    run(*runtime, truncate_open.close());

    require(run_error(*runtime,
                      client.open("/bad-access-mode", O_ACCMODE, 0644)) ==
                std::errc::invalid_argument,
            "access mode 3 was accepted");
    require(run_error(*runtime,
                      client.open("/unsupported-sync", O_RDWR | O_SYNC,
                                  0644))
                    .value() == EOPNOTSUPP,
            "O_SYNC was accepted without synchronous-write semantics");
    require(run_error(*runtime,
                      client.open("/unknown-open-flag",
                                  O_RDWR | static_cast<int>(0x40000000U),
                                  0644))
                    .value() == EOPNOTSUPP,
            "unknown open flag was accepted");
#ifdef O_PATH
    require(run_error(*runtime,
                      client.open("/unsupported-path", O_PATH, 0644))
                    .value() == EOPNOTSUPP,
            "O_PATH was accepted");
#endif
#ifdef O_TMPFILE
    require(run_error(*runtime,
                      client.open("/", O_TMPFILE | O_RDWR, 0644))
                    .value() == EOPNOTSUPP,
            "O_TMPFILE was accepted");
#endif

    auto append_seed = run(
        *runtime, client.open("/append-file", O_CREAT | O_RDWR | O_TRUNC, 0644));
    const std::array<std::byte, 1> seed{std::byte{'A'}};
    require(run(*runtime, append_seed.write(seed)) == seed.size(),
            "append seed write failed");
    run(*runtime, append_seed.close());
    auto append_left =
        run(*runtime, client.open("/append-file",
                                  O_WRONLY | O_APPEND | O_CLOEXEC));
    auto append_right =
        run(*runtime, client.open("/append-file", O_WRONLY | O_APPEND));
    const int initial_flags = run(*runtime, append_left.get_flags());
    require((initial_flags & O_ACCMODE) == O_WRONLY &&
                (initial_flags & O_APPEND) != 0 &&
                (initial_flags & O_CLOEXEC) == 0,
            "F_GETFL lost status flags or exposed O_CLOEXEC");
    int requested_status = O_APPEND | O_NONBLOCK;
    int unsupported_status = 0;
#ifdef O_DIRECT
    requested_status |= O_DIRECT;
    unsupported_status |= O_DIRECT;
#endif
#ifdef O_ASYNC
    requested_status |= O_ASYNC;
    unsupported_status |= O_ASYNC;
#endif
#ifdef O_NOATIME
    requested_status |= O_NOATIME;
    unsupported_status |= O_NOATIME;
#endif
    run(*runtime, append_left.set_flags(requested_status));
    const int updated_flags = run(*runtime, append_left.get_flags());
    require((updated_flags & O_ACCMODE) == O_WRONLY &&
                (updated_flags & (O_APPEND | O_NONBLOCK)) ==
                    (O_APPEND | O_NONBLOCK) &&
                (updated_flags & unsupported_status) == 0,
            "F_SETFL exposed an unimplemented mutable status flag");
    const std::array<std::byte, 1> left_byte{std::byte{'L'}};
    const std::array<std::byte, 1> right_byte{std::byte{'R'}};
    auto left_write = runtime->submit(append_left.write(left_byte));
    auto right_write = runtime->submit(append_right.write(right_byte));
    require(left_write && right_write, "append submit failed");
    auto left_joined = std::move(left_write).value().join();
    auto right_joined = std::move(right_write).value().join();
    require(left_joined && right_joined && left_joined.value() &&
                right_joined.value() && left_joined.value().value() == 1 &&
                right_joined.value().value() == 1,
            "atomic append writes failed");
    run(*runtime, append_left.close());
    run(*runtime, append_right.close());
    auto append_reader =
        run(*runtime, client.open("/append-file", O_RDONLY));
    std::array<std::byte, 3> append_contents{};
    require(run(*runtime, append_reader.read(append_contents)) ==
                append_contents.size(),
            "append readback length mismatch");
    require(append_contents[0] == std::byte{'A'} &&
                ((append_contents[1] == std::byte{'L'} &&
                  append_contents[2] == std::byte{'R'}) ||
                 (append_contents[1] == std::byte{'R'} &&
                  append_contents[2] == std::byte{'L'})),
            "O_APPEND overwrote data or was not atomic");
    run(*runtime, append_reader.close());

    require(run_error(*runtime, client.stat("/missing-stat")) ==
                std::errc::no_such_file_or_directory,
            "missing stat did not return ENOENT");
    require(run_error(*runtime, client.open_directory("/missing-directory")) ==
                std::errc::no_such_file_or_directory,
            "missing opendir did not return ENOENT");

    auto cleanup_on_shutdown = run(
        *runtime, client.open("/cleanup-on-shutdown", O_CREAT | O_RDWR, 0644));
    run(*runtime, client.shutdown());
    (void)cleanup_on_shutdown;
    runtime->request_stop();
    require(static_cast<bool>(runtime->join()), "client Runtime::join failed");
    return 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "client failure: %s\n", error.what());
    return 1;
  }
}

}  // namespace

extern "C" {

int orchfs_open(const char* pathname, int flags, ...) {
  record_legacy_call();
  mode_t mode = 0;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = static_cast<mode_t>(va_arg(arguments, int));
    va_end(arguments);
  }
  const int fd = ::open(host_path(pathname).c_str(), legacy_open_flags(flags),
                        mode);
  remember_file_handle(fd, pathname == nullptr ? "" : pathname);
  return fd;
}

int orchfs_openat(int dir_handle, const char* pathname, int flags, ...) {
  record_legacy_call();
  mode_t mode = 0;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = static_cast<mode_t>(va_arg(arguments, int));
    va_end(arguments);
  }
  const int fd =
      ::openat(dir_handle, pathname, legacy_open_flags(flags), mode);
  remember_file_handle(fd, pathname == nullptr ? "" : pathname);
  return fd;
}

int orchfs_close(int fd) {
  record_legacy_call();
  if (fail_this_close_once(fd)) {
    errno = EIO;
    return -1;
  }
  const int result = ::close(fd);
  if (result == 0) {
    forget_file_handle(fd);
  }
  return result;
}
int64_t orchfs_pwrite(int fd, const void* buffer, int64_t length,
                      int64_t offset) {
  record_host_io_call();
  const auto result = ::pwrite(fd, buffer, static_cast<std::size_t>(length),
                               offset);
  return result >= 0 && file_path_is(fd, "/over-write") ? length + 1 : result;
}
int64_t orchfs_pread(int fd, void* buffer, int64_t length, int64_t offset) {
  record_host_io_call();
  const auto result = ::pread(fd, buffer, static_cast<std::size_t>(length),
                              offset);
  return result >= 0 && file_path_is(fd, "/over-read") ? length + 1 : result;
}
int orchfs_mkdir(const char* pathname, uint16_t mode) {
  record_legacy_call();
  return ::mkdir(host_path(pathname).c_str(), mode);
}
int orchfs_rmdir(const char* pathname) {
  record_legacy_call();
  return ::rmdir(host_path(pathname).c_str());
}
int orchfs_unlink(const char* pathname) {
  record_legacy_call();
  return ::unlink(host_path(pathname).c_str());
}
int orchfs_fstatfs(int fd, struct statfs* value) {
  record_legacy_call();
  return ::fstatfs(fd, value);
}
int orchfs_stat(const char* pathname, struct stat* value) {
  record_legacy_call();
  return ::stat(host_path(pathname).c_str(), value);
}
int orchfs_fstat(int fd, struct stat* value) {
  record_legacy_call();
  return ::fstat(fd, value);
}
int orchfs_truncate(const char* pathname, size_t length) {
  record_legacy_call();
  return ::truncate(host_path(pathname).c_str(), static_cast<off_t>(length));
}
int orchfs_ftruncate(int fd, size_t length) {
  record_legacy_call();
  return ::ftruncate(fd, static_cast<off_t>(length));
}
int orchfs_fsync(int fd) {
  record_legacy_call();
  return ::fsync(fd);
}
int orchfs_rename(const char* old_path, const char* new_path) {
  record_legacy_call();
  auto split = [](std::string_view path) {
    const auto slash = path.find_last_of('/');
    return std::pair{
        slash == std::string_view::npos
            ? std::string_view(".")
            : (slash == 0 ? std::string_view("/") : path.substr(0, slash)),
        slash == std::string_view::npos ? path : path.substr(slash + 1)};
  };
  if (old_path == nullptr || new_path == nullptr || *old_path == '\0' ||
      *new_path == '\0') {
    errno = ENOENT;
    return -1;
  }
  const auto [old_parent, old_name] = split(old_path);
  const auto [new_parent, new_name] = split(new_path);
  if (old_name.empty() || new_name.empty() || old_name == "." ||
      old_name == ".." || new_name == "." || new_name == "..") {
    errno = EINVAL;
    return -1;
  }
  if (old_name.size() > 230 || new_name.size() > 230) {
    errno = ENAMETOOLONG;
    return -1;
  }
  if (old_parent != new_parent) {
    errno = EXDEV;
    return -1;
  }
  if (::access(host_path(new_path).c_str(), F_OK) == 0) {
    errno = EEXIST;
    return -1;
  }
  return ::rename(host_path(old_path).c_str(), host_path(new_path).c_str());
}

int submit_read_data_from_devs(
    void* destination, int64_t length, int64_t offset,
    void (*completion)(void*, int, size_t), void* context) {
  (void)destination;
  (void)offset;
  completion(context, 0, static_cast<size_t>(length));
  return 0;
}

int submit_write_data_to_devs(
    const void* source, int64_t length, int64_t offset,
    void (*completion)(void*, int, size_t), void* context) {
  (void)source;
  (void)offset;
  completion(context, 0, static_cast<size_t>(length));
  return 0;
}

int submit_device_sync(void (*completion)(void*, int, size_t), void* context) {
  completion(context, 0, 0);
  return 0;
}

int orchfs_device_register_dma_region(void* address, size_t length) {
  return address != nullptr && length != 0 ? 0 : EINVAL;
}

int orchfs_device_unregister_dma_region(void* address, size_t length) {
  return address != nullptr && length != 0 ? 0 : EINVAL;
}
}  // extern "C"

int main(int argc, char* argv[]) {
  if (argc == 3 && std::strcmp(argv[1], "--benchmark-client") == 0) {
    return run_benchmark_client(argv[2]);
  }

  const char* benchmark_value = std::getenv("ORCHFS_RUN_ASYNC_BENCHMARK");
  const bool run_benchmark =
      benchmark_value != nullptr && std::strcmp(benchmark_value, "0") != 0;
  const char* skip_wrapper_value = std::getenv("ORCHFS_SKIP_WRAPPER_TEST");
  const bool skip_wrapper = skip_wrapper_value != nullptr &&
      std::strcmp(skip_wrapper_value, "0") != 0;

  std::array<char, 64> root_template{};
  std::strcpy(root_template.data(), "/tmp/orchfs-async-e2e-XXXXXX");
  char* created_root = ::mkdtemp(root_template.data());
  if (created_root == nullptr) {
    std::perror("mkdtemp");
    return 1;
  }
  root_path = created_root;
  const std::string endpoint = root_path + "/control.sock";

  const pid_t child = ::fork();
  if (child < 0) {
    std::perror("fork");
    return 1;
  }
  if (child == 0) {
    const int result = run_client(endpoint);
    std::fflush(nullptr);
    _exit(result);
  }

  const pid_t wrapper_child = skip_wrapper ? -1 : ::fork();
  if (!skip_wrapper && wrapper_child < 0) {
    std::perror("fork wrapper client");
    (void)::kill(child, SIGKILL);
    (void)::waitpid(child, nullptr, 0);
    return 1;
  }
  if (!skip_wrapper && wrapper_child == 0) {
    const int wrapper_result = run_wrapper_client(endpoint);
    std::fflush(nullptr);
    _exit(wrapper_result);
  }

  int result = 1;
  try {
    orchfs::async::RuntimeOptions runtime_options;
    runtime_options.worker_count = run_benchmark ? benchmark_stream_count : 3;
    if (run_benchmark) {
      runtime_options.cpu_set =
          benchmark_cpu_set("ORCHFS_ASYNC_BENCHMARK_SERVER_CPUS");
      runtime_options.spin_before_park = benchmark_spin_count();
    }
    auto created = orchfs::async::Runtime::create(std::move(runtime_options));
    require(static_cast<bool>(created), "server Runtime::create failed");
    auto runtime = std::move(created).value();

    orchfs::async::ServerOptions server_options;
    server_options.endpoint = endpoint;
    server_options.lane_count = 4;
    server_options.ring_capacity = run_benchmark ? 64 : 8;
    server_options.data_slot_size =
        run_benchmark
            ? benchmark_block_size + sizeof(orchfs::async::RpcRequest)
            : 4096;
    server_options.filesystem = std::make_shared<HostAsyncFilesystem>();
    auto started = orchfs::async::Server::start(*runtime, server_options);
    require(static_cast<bool>(started), "Server::start failed");
    auto server = std::move(started).value();

    int status = 0;
    require(::waitpid(child, &status, 0) == child, "waitpid failed");
    require(WIFEXITED(status) && WEXITSTATUS(status) == 0,
            "client process failed");
    if (!skip_wrapper) {
      int wrapper_status = 0;
      require(::waitpid(wrapper_child, &wrapper_status, 0) == wrapper_child,
              "waitpid wrapper client failed");
      require(WIFEXITED(wrapper_status) && WEXITSTATUS(wrapper_status) == 0,
              "wrapper client process failed");
    }

    if (run_benchmark) {
      const pid_t benchmark_child = ::fork();
      require(benchmark_child >= 0, "fork benchmark client failed");
      if (benchmark_child == 0) {
        ::execl("/proc/self/exe", argv[0], "--benchmark-client",
                endpoint.c_str(), static_cast<char*>(nullptr));
        std::perror("exec benchmark client");
        _exit(127);
      }
      int benchmark_status = 0;
      require(::waitpid(benchmark_child, &benchmark_status, 0) ==
                  benchmark_child,
              "waitpid benchmark client failed");
      require(WIFEXITED(benchmark_status) &&
                  WEXITSTATUS(benchmark_status) == 0,
              "benchmark client process failed");
    }

    const auto cleanup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (active_legacy_handles.load(std::memory_order_acquire) != 0 &&
           std::chrono::steady_clock::now() < cleanup_deadline) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    require(active_legacy_handles.load(std::memory_order_acquire) == 0,
            "disconnect or explicit shutdown leaked legacy handles");
    require(legacy_call_count.load(std::memory_order_relaxed) != 0,
            "legacy mocks were not exercised");
    require(legacy_called_on_runtime.load(std::memory_order_relaxed),
            "AsyncFilesystem mock was not driven by the Runtime");
    require(namespace_handle_call_count.load(std::memory_order_relaxed) >= 2,
            "handle-relative namespace paths were not exercised");
    require(!namespace_handle_called_off_owner.load(std::memory_order_relaxed),
            "handle-relative namespace work escaped Runtime worker 0");
    require(lifecycle_call_count.load(std::memory_order_relaxed) != 0,
            "filesystem lifecycle paths were not exercised");
    require(!lifecycle_called_off_owner.load(std::memory_order_relaxed),
            "filesystem lifecycle work escaped Runtime worker 0");
    require(inode_owner_call_count.load(std::memory_order_relaxed) != 0,
            "inode-owned filesystem paths were not exercised");
    require(!inode_called_off_owner.load(std::memory_order_relaxed),
            "inode filesystem work escaped its Runtime owner");

    exercise_completion_backpressure(endpoint);

    const int stalled_client = connect_without_hello(endpoint);
    const auto healthy_connect_start = std::chrono::steady_clock::now();
    {
      auto healthy_transport = connect_raw(endpoint);
      require(static_cast<bool>(healthy_transport),
              "healthy client did not pass a stalled handshake");
    }
    require(std::chrono::steady_clock::now() - healthy_connect_start <
                std::chrono::seconds(2),
            "stalled hello blocked later clients indefinitely");
    ::close(stalled_client);

    auto saturated_transport = make_saturated_session(endpoint);
    require(active_legacy_handles.load(std::memory_order_acquire) == 1,
            "server-stop cleanup fixture was not opened");
    const int stop_stalled_client = connect_without_hello(endpoint);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    const auto server_stop_start = std::chrono::steady_clock::now();
    server->request_stop();
    require(static_cast<bool>(server->join()), "Server::join failed");
    require(std::chrono::steady_clock::now() - server_stop_start <
                std::chrono::seconds(2),
            "Server::join blocked on a stalled hello or saturated CQ");
    ::close(stop_stalled_client);
    require(active_legacy_handles.load(std::memory_order_acquire) == 0,
            "Server::stop leaked a legacy handle");
    runtime->request_stop();
    require(static_cast<bool>(runtime->join()), "server Runtime::join failed");
    result = 0;
  } catch (const std::exception& error) {
    std::fprintf(stderr, "server failure: %s\n", error.what());
    (void)::kill(child, SIGKILL);
    if (wrapper_child > 0) {
      (void)::kill(wrapper_child, SIGKILL);
    }
    (void)::waitpid(child, nullptr, 0);
    if (wrapper_child > 0) {
      (void)::waitpid(wrapper_child, nullptr, 0);
    }
  }

  std::error_code cleanup_error;
  std::filesystem::remove_all(root_path, cleanup_error);
  if (result == 0) {
    std::puts("async client/server tests passed");
  }
  return result;
}
