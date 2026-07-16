#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "async_adapter.h"

#include "orchfs/async/client.hpp"
#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdarg>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <optional>
#include <sched.h>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

namespace {

using orchfs::async::Client;
using orchfs::async::ClientOptions;
using orchfs::async::DirEntry;
using orchfs::async::Directory;
using orchfs::async::Errc;
using orchfs::async::File;
using orchfs::async::FileStat;
using orchfs::async::FileSystemStat;
using orchfs::async::Result;
using orchfs::async::Runtime;
using orchfs::async::RuntimeOptions;
using orchfs::async::Task;

constexpr int kMaximumLocalFd = 1048575;
constexpr std::size_t kDirectoryBatchSize = 32;
constexpr std::uint64_t kDirectoryTokenMagic = 0x4f52434844495231ULL;

struct FileSlot {
  FileSlot(File value, std::optional<Directory> directory_value,
           std::string opened_path, int flags)
      : file(std::move(value)),
        directory(std::move(directory_value)),
        directory_checked(directory.has_value()),
        path(std::move(opened_path)),
        open_flags(flags),
        descriptor_flags((flags & O_CLOEXEC) != 0 ? FD_CLOEXEC : 0) {}

  std::shared_mutex mutex;
  File file;
  std::optional<Directory> directory;
  bool directory_checked{};
  std::string path;
  int open_flags{};
  int descriptor_flags{};
  bool closed{};
};

struct DirectoryToken {
  std::uint64_t magic{kDirectoryTokenMagic};
  pid_t owner_pid{::getpid()};
};

struct DirectorySlot {
  explicit DirectorySlot(Directory value) : directory(std::move(value)) {}

  std::mutex mutex;
  Directory directory;
  std::array<DirEntry, kDirectoryBatchSize> batch;
  std::size_t cursor{};
  std::size_t count{};
  bool closed{};
  struct dirent entry {};
  struct dirent64 entry64 {};
};

struct AdapterState {
  pid_t owner_pid{::getpid()};
  std::shared_mutex lifecycle_mutex;
  std::unique_ptr<Runtime> runtime;
  std::optional<Client> client;
  std::error_code initialization_error;
  bool ever_connected{};

  std::mutex files_mutex;
  std::unordered_map<int, std::shared_ptr<FileSlot>> files;
  int next_fd{3};

  std::mutex directories_mutex;
  std::unordered_map<DIR*, std::shared_ptr<DirectorySlot>> directories;
};

AdapterState& state() {
  // The LD_PRELOAD destructor performs the ordered teardown.  Keeping the
  // state object itself alive avoids static-destruction-order races with libc.
  static std::atomic<AdapterState*> value{new AdapterState};
  const pid_t process = ::getpid();
  for (;;) {
    AdapterState* current = value.load(std::memory_order_acquire);
    if (current->owner_pid == process) {
      return *current;
    }

    // Only the calling thread survives fork(). Parent Runtime threads and any
    // mutexes they held cannot be joined or safely destroyed in the child, so
    // abandon that process-local state and lazily establish a fresh session.
    // CAS also handles multiple child threads making their first adapter call
    // concurrently without racing on the process-local state pointer.
    auto* replacement = new AdapterState;
    replacement->ever_connected = current->ever_connected;
    if (value.compare_exchange_strong(current, replacement,
                                      std::memory_order_acq_rel,
                                      std::memory_order_acquire)) {
      return *replacement;
    }
    delete replacement;
  }
}

int errno_from_error(std::error_code error) noexcept {
  if (!error) {
    return EIO;
  }
  if (error.category() == std::generic_category() ||
      error.category() == std::system_category()) {
    return error.value() > 0 ? error.value() : EIO;
  }
  if (error.category() == orchfs::async::error_category()) {
    switch (static_cast<Errc>(error.value())) {
      case Errc::runtime_stopping:
        return ESHUTDOWN;
      case Errc::runtime_already_exists:
        return EALREADY;
      case Errc::invalid_handle:
      case Errc::already_consumed:
        return EBADF;
      case Errc::invalid_task:
      case Errc::invalid_worker:
      case Errc::invalid_range:
        return EINVAL;
      case Errc::join_from_worker:
        return EDEADLK;
      case Errc::not_in_runtime:
      case Errc::wrong_runtime:
      case Errc::pinned_to_worker:
        return EPROTO;
    }
  }
  return EIO;
}

template <typename Return>
Return fail(std::error_code error, Return failure_value) noexcept {
  errno = errno_from_error(error);
  return failure_value;
}

template <typename Return>
Return fail(int error, Return failure_value) noexcept {
  errno = error;
  return failure_value;
}

template <typename T>
Result<T> run(AdapterState& adapter, Task<Result<T>> task) {
  if (!adapter.runtime) {
    const auto error = adapter.initialization_error
                           ? adapter.initialization_error
                           : std::make_error_code(std::errc::not_connected);
    return Result<T>::failure(error);
  }
  auto submitted = adapter.runtime->submit(std::move(task));
  if (!submitted) {
    return Result<T>::failure(submitted.error());
  }
  auto joined = std::move(submitted).value().join();
  if (!joined) {
    return Result<T>::failure(joined.error());
  }
  return std::move(joined).value();
}

bool ready(const AdapterState& adapter) noexcept {
  return adapter.runtime && adapter.client && adapter.client->valid();
}

std::size_t configured_worker_count() noexcept {
  constexpr std::size_t kDefaultWorkerCount = 4;
  const char* text = std::getenv("ORCHFS_CLIENT_WORKERS");
  if (!text || !*text) {
    cpu_set_t affinity;
    CPU_ZERO(&affinity);
    if (::sched_getaffinity(0, sizeof(affinity), &affinity) == 0) {
      const auto available = static_cast<std::size_t>(CPU_COUNT(&affinity));
      if (available != 0) {
        return std::min(kDefaultWorkerCount, available);
      }
    }
    return kDefaultWorkerCount;
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    return kDefaultWorkerCount;
  }
  return static_cast<std::size_t>(parsed);
}

std::size_t configured_size(const char* name, std::size_t fallback) noexcept {
  const char* text = std::getenv(name);
  if (!text || !*text) {
    return fallback;
  }
  char* end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    return fallback;
  }
  return static_cast<std::size_t>(parsed);
}

std::shared_ptr<FileSlot> find_file(AdapterState& adapter, int fd) {
  std::lock_guard lock(adapter.files_mutex);
  const auto found = adapter.files.find(fd);
  return found == adapter.files.end() ? nullptr : found->second;
}

int insert_file(AdapterState& adapter, std::shared_ptr<FileSlot> slot) {
  std::lock_guard lock(adapter.files_mutex);
  if (adapter.files.size() >= static_cast<std::size_t>(kMaximumLocalFd - 2)) {
    return -1;
  }
  for (int attempts = 0; attempts < kMaximumLocalFd; ++attempts) {
    int candidate = adapter.next_fd++;
    if (adapter.next_fd > kMaximumLocalFd) {
      adapter.next_fd = 3;
    }
    if (!adapter.files.contains(candidate)) {
      adapter.files.emplace(candidate, std::move(slot));
      return candidate;
    }
  }
  return -1;
}

std::shared_ptr<DirectorySlot> find_directory(AdapterState& adapter,
                                               DIR* directory) {
  std::lock_guard lock(adapter.directories_mutex);
  const auto found = adapter.directories.find(directory);
  return found == adapter.directories.end() ? nullptr : found->second;
}

void populate_stat(const FileStat& input, struct stat& output) noexcept {
  std::memset(&output, 0, sizeof(output));
  output.st_dev = static_cast<dev_t>(input.device);
  output.st_ino = static_cast<ino_t>(input.inode);
  output.st_mode = static_cast<mode_t>(input.mode);
  output.st_nlink = static_cast<nlink_t>(input.link_count);
  output.st_uid = static_cast<uid_t>(input.uid);
  output.st_gid = static_cast<gid_t>(input.gid);
  output.st_rdev = static_cast<dev_t>(input.rdev);
  output.st_size = static_cast<off_t>(input.size);
  output.st_blksize = static_cast<blksize_t>(input.block_size);
  output.st_blocks = static_cast<blkcnt_t>(input.blocks);
  output.st_atim.tv_sec = static_cast<time_t>(input.atime_seconds);
  output.st_atim.tv_nsec = static_cast<long>(input.atime_nanoseconds);
  output.st_mtim.tv_sec = static_cast<time_t>(input.mtime_seconds);
  output.st_mtim.tv_nsec = static_cast<long>(input.mtime_nanoseconds);
  output.st_ctim.tv_sec = static_cast<time_t>(input.ctime_seconds);
  output.st_ctim.tv_nsec = static_cast<long>(input.ctime_nanoseconds);
}

void populate_statfs(const FileSystemStat& input,
                     struct statfs& output) noexcept {
  std::memset(&output, 0, sizeof(output));
  output.f_type = static_cast<decltype(output.f_type)>(input.type);
  output.f_bsize = static_cast<decltype(output.f_bsize)>(input.block_size);
  output.f_blocks = static_cast<decltype(output.f_blocks)>(input.blocks);
  output.f_bfree = static_cast<decltype(output.f_bfree)>(input.blocks_free);
  output.f_bavail =
      static_cast<decltype(output.f_bavail)>(input.blocks_available);
  output.f_files = static_cast<decltype(output.f_files)>(input.files);
  output.f_ffree = static_cast<decltype(output.f_ffree)>(input.files_free);
  output.f_namelen = static_cast<decltype(output.f_namelen)>(input.name_length);
  output.f_frsize = static_cast<decltype(output.f_frsize)>(input.fragment_size);
  output.f_flags = static_cast<decltype(output.f_flags)>(input.flags);
}

bool valid_path(const char* path) noexcept {
  return path != nullptr && *path != '\0';
}

std::optional<std::string> joined_openat_path(const FileSlot& directory,
                                              const char* path) {
  try {
    return (std::filesystem::path(directory.path) / path)
        .lexically_normal()
        .generic_string();
  } catch (const std::bad_alloc&) {
    errno = ENOMEM;
    return std::nullopt;
  } catch (...) {
    errno = EINVAL;
    return std::nullopt;
  }
}

std::error_code ensure_directory_handle(AdapterState& adapter,
                                        FileSlot& slot) {
  if (slot.directory) {
    return {};
  }
  if (slot.directory_checked) {
    return std::make_error_code(std::errc::not_a_directory);
  }

  auto opened = run(adapter, adapter.client->open_directory(slot.file));
  if (opened) {
    slot.directory.emplace(std::move(opened).value());
    slot.directory_checked = true;
    return {};
  }

  if (errno_from_error(opened.error()) == ENOTDIR) {
    // Cache only the stable inode-type result. Transport failures remain
    // retryable after the adapter reconnects.
    slot.directory_checked = true;
  }
  return opened.error();
}

std::optional<std::string> path_for_openat(AdapterState& adapter, int dirfd,
                                           const char* path) {
  if (!valid_path(path)) {
    errno = path ? ENOENT : EFAULT;
    return std::nullopt;
  }
  if (*path == '/' || dirfd == AT_FDCWD) {
    return std::string(path);
  }
  auto directory = find_file(adapter, dirfd);
  if (!directory) {
    errno = EBADF;
    return std::nullopt;
  }

  {
    std::shared_lock lock(directory->mutex);
    if (directory->closed) {
      errno = EBADF;
      return std::nullopt;
    }
    if (directory->directory) {
      return joined_openat_path(*directory, path);
    }
    if (directory->directory_checked) {
      errno = ENOTDIR;
      return std::nullopt;
    }
  }

  // Resolve the capability lazily from the already-open File handle. This
  // preserves the inode selected by open(2), even if its pathname is renamed
  // or recreated before the first openat(2), without taxing ordinary file
  // opens with a second RPC.
  std::unique_lock lock(directory->mutex);
  if (directory->closed) {
    errno = EBADF;
    return std::nullopt;
  }
  if (const auto error = ensure_directory_handle(adapter, *directory); error) {
    errno = errno_from_error(error);
    return std::nullopt;
  }
  return joined_openat_path(*directory, path);
}

template <typename Entry>
void populate_directory_entry(const DirEntry& input, Entry& output) noexcept {
  std::memset(&output, 0, sizeof(output));
  output.d_ino = static_cast<decltype(output.d_ino)>(input.inode);
  output.d_off = static_cast<decltype(output.d_off)>(input.offset);
  output.d_type = input.type;
  const std::size_t length =
      std::min(input.name.size(), sizeof(output.d_name) - 1);
  std::memcpy(output.d_name, input.name.data(), length);
  output.d_name[length] = '\0';
  const std::size_t record_length = offsetof(Entry, d_name) + length + 1;
  output.d_reclen = static_cast<decltype(output.d_reclen)>(record_length);
}

template <typename Entry>
Entry* read_directory_entry(AdapterState& adapter, DIR* directory,
                            Entry DirectorySlot::*entry_member) {
  auto slot = find_directory(adapter, directory);
  if (!slot) {
    return fail(EBADF, static_cast<Entry*>(nullptr));
  }
  std::lock_guard lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<Entry*>(nullptr));
  }
  if (slot->cursor == slot->count) {
    slot->cursor = 0;
    auto result = run(adapter, slot->directory.next_batch(slot->batch));
    if (!result) {
      return fail(result.error(), static_cast<Entry*>(nullptr));
    }
    slot->count = std::move(result).value();
    if (slot->count == 0) {
      errno = 0;
      return nullptr;
    }
  }
  Entry& output = slot.get()->*entry_member;
  populate_directory_entry(slot->batch[slot->cursor++], output);
  return &output;
}

}  // namespace

extern "C" int orchfs_async_adapter_init(void) {
  auto& adapter = state();
  std::unique_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (ready(adapter)) {
    return 0;
  }

  try {
    bool created_runtime = false;
    if (!adapter.runtime) {
      RuntimeOptions runtime_options;
      runtime_options.worker_count = configured_worker_count();
      auto runtime = Runtime::create(std::move(runtime_options));
      if (!runtime) {
        adapter.initialization_error = runtime.error();
        return fail(runtime.error(), -1);
      }
      adapter.runtime = std::move(runtime).value();
      created_runtime = true;
    } else {
      // A transport failure invalidates Client but not the process Runtime.
      // Reuse those workers for a fresh session instead of violating the
      // one-Runtime-per-process invariant.
      adapter.client.reset();
    }

    ClientOptions client_options;
    if (const char* endpoint = std::getenv("ORCHFS_ASYNC_ENDPOINT");
        endpoint && *endpoint) {
      client_options.endpoint = endpoint;
    }
    client_options.ring_capacity = configured_size(
        "ORCHFS_IPC_RING_CAPACITY", client_options.ring_capacity);
    client_options.data_slot_size = configured_size(
        "ORCHFS_IPC_DATA_SLOT_SIZE", client_options.data_slot_size);
    auto connected = run(adapter, Client::connect(*adapter.runtime,
                                                   std::move(client_options)));
    if (!connected) {
      adapter.initialization_error = connected.error();
      if (created_runtime) {
        adapter.runtime->request_stop();
        (void)adapter.runtime->join();
        adapter.runtime.reset();
      }
      return fail(adapter.initialization_error, -1);
    }
    adapter.client.emplace(std::move(connected).value());
    adapter.ever_connected = true;
    adapter.initialization_error.clear();
    return 0;
  } catch (const std::bad_alloc&) {
    adapter.initialization_error =
        std::make_error_code(std::errc::not_enough_memory);
  } catch (...) {
    adapter.initialization_error = std::make_error_code(std::errc::io_error);
  }

  if (adapter.runtime) {
    adapter.runtime->request_stop();
    (void)adapter.runtime->join();
    adapter.runtime.reset();
  }
  adapter.client.reset();
  return fail(adapter.initialization_error, -1);
}

extern "C" int orchfs_async_adapter_ready(void) {
  auto& adapter = state();
  {
    std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
    if (ready(adapter)) {
      return 1;
    }
  }
  // Pathname wrappers use this as the lazy reconnect point. init() serializes
  // concurrent callers and reuses the process Runtime after a dead session.
  return orchfs_async_adapter_init() == 0 ? 1 : 0;
}

extern "C" int orchfs_async_adapter_allow_host_fallback(
    int adapter_was_ready, int error_number) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (adapter_was_ready) {
    // The operation reached a live session, so ENOENT is a filesystem lookup
    // result rather than a missing control socket from connect(2).
    return error_number == ENOENT;
  }
  return !adapter.ever_connected;
}

extern "C" void orchfs_async_adapter_shutdown(void) {
  auto& adapter = state();
  std::unique_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!adapter.runtime) {
    adapter.client.reset();
    return;
  }

  std::unordered_map<int, std::shared_ptr<FileSlot>> files;
  {
    std::lock_guard lock(adapter.files_mutex);
    files.swap(adapter.files);
  }
  for (auto& [fd, slot] : files) {
    (void)fd;
    std::unique_lock lock(slot->mutex);
    (void)run(adapter, slot->file.close());
    if (slot->directory) {
      (void)run(adapter, slot->directory->close());
      slot->directory.reset();
    }
  }

  std::unordered_map<DIR*, std::shared_ptr<DirectorySlot>> directories;
  {
    std::lock_guard lock(adapter.directories_mutex);
    directories.swap(adapter.directories);
  }
  for (auto& [token, slot] : directories) {
    {
      std::lock_guard lock(slot->mutex);
      (void)run(adapter, slot->directory.close());
    }
    delete reinterpret_cast<DirectoryToken*>(token);
  }

  // Destroy every slot while Runtime is still alive. A failed remote close
  // leaves RAII handles that perform best-effort detached cleanup from their
  // destructors; letting these maps outlive runtime.reset() would leave their
  // Session::runtime_ pointers dangling.
  files.clear();
  directories.clear();

  if (adapter.client && adapter.client->valid()) {
    (void)run(adapter, adapter.client->shutdown());
  }
  adapter.client.reset();
  adapter.runtime->request_stop();
  (void)adapter.runtime->join();
  adapter.runtime.reset();
}

extern "C" int orchfs_async_open(const char* path, int flags, ...) {
  mode_t mode = 0644;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = va_arg(arguments, mode_t);
    va_end(arguments);
  }
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }

  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto opened = run(adapter, adapter.client->open(path, flags, mode));
  if (!opened) {
    return fail(opened.error(), -1);
  }

  try {
    auto slot = std::make_shared<FileSlot>(std::move(opened).value(),
                                          std::nullopt,
                                          std::string(path), flags);
    const int fd = insert_file(adapter, std::move(slot));
    if (fd < 0) {
      return fail(EMFILE, -1);
    }
    return fd;
  } catch (const std::bad_alloc&) {
    return fail(ENOMEM, -1);
  } catch (...) {
    return fail(EIO, -1);
  }
}

extern "C" int orchfs_async_openat(int dirfd, const char* path, int flags,
                                    ...) {
  mode_t mode = 0644;
  if ((flags & O_CREAT) != 0) {
    va_list arguments;
    va_start(arguments, flags);
    mode = va_arg(arguments, mode_t);
    va_end(arguments);
  }

  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto combined_path = path_for_openat(adapter, dirfd, path);
  if (!combined_path) {
    return -1;
  }
  Result<File> opened = [&]() {
    if (*path == '/' || dirfd == AT_FDCWD) {
      return run(adapter,
                 adapter.client->open(*combined_path, flags, mode));
    }
    auto parent = find_file(adapter, dirfd);
    if (!parent) {
      return Result<File>::failure(
          std::make_error_code(std::errc::bad_file_descriptor));
    }
    std::shared_lock parent_lock(parent->mutex);
    if (parent->closed) {
      return Result<File>::failure(
          std::make_error_code(std::errc::bad_file_descriptor));
    }
    if (!parent->directory) {
      return Result<File>::failure(
          std::make_error_code(std::errc::not_a_directory));
    }
    return run(adapter, adapter.client->open_at(
                            *parent->directory, std::string(path), flags, mode));
  }();
  if (!opened) {
    return fail(opened.error(), -1);
  }

  try {
    auto slot = std::make_shared<FileSlot>(std::move(opened).value(),
                                          std::nullopt,
                                          std::move(*combined_path), flags);
    const int fd = insert_file(adapter, std::move(slot));
    if (fd < 0) {
      return fail(EMFILE, -1);
    }
    return fd;
  } catch (const std::bad_alloc&) {
    return fail(ENOMEM, -1);
  } catch (...) {
    return fail(EIO, -1);
  }
}

extern "C" int orchfs_async_close(int fd) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  std::unique_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    // A dead transport makes a remote retry impossible; the server's
    // once-only session cleanup owns the peer handle. Closing the local proxy
    // must still retire it instead of leaking it until a pathname reconnect.
    slot->closed = true;
    slot->directory.reset();
    {
      std::lock_guard files_lock(adapter.files_mutex);
      const auto found = adapter.files.find(fd);
      if (found != adapter.files.end() && found->second == slot) {
        adapter.files.erase(found);
      }
    }
    return 0;
  }
  auto result = run(adapter, slot->file.close());
  if (!result) {
    // close(2) failures leave this compatibility handle usable so callers can
    // observe the error and retry.  No local state is discarded first.
    return fail(result.error(), -1);
  }
  if (slot->directory) {
    // This is an auxiliary handle used solely to preserve openat inode
    // semantics.  The user-visible File close already succeeded.
    (void)run(adapter, slot->directory->close());
    slot->directory.reset();
  }
  slot->closed = true;
  {
    std::lock_guard files_lock(adapter.files_mutex);
    const auto found = adapter.files.find(fd);
    if (found != adapter.files.end() && found->second == slot) {
      adapter.files.erase(found);
    }
  }
  return 0;
}

extern "C" ssize_t orchfs_async_read(int fd, void* buffer, size_t length) {
  if (!buffer && length != 0) {
    return fail(EFAULT, static_cast<ssize_t>(-1));
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<ssize_t>(-1));
  }
  std::unique_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  auto result = run(adapter, slot->file.read(
                                 {static_cast<std::byte*>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_write(int fd, const void* buffer,
                                       size_t length) {
  if (!buffer && length != 0) {
    return fail(EFAULT, static_cast<ssize_t>(-1));
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<ssize_t>(-1));
  }
  std::unique_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  auto result = run(adapter, slot->file.write(
                                 {static_cast<const std::byte*>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_pread(int fd, void* buffer, size_t length,
                                       off_t offset) {
  if ((!buffer && length != 0) || offset < 0) {
    return fail(!buffer && length != 0 ? EFAULT : EINVAL,
                static_cast<ssize_t>(-1));
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<ssize_t>(-1));
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  auto result = run(adapter, slot->file.read_at(
                                 static_cast<std::uint64_t>(offset),
                                 {static_cast<std::byte*>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_pwrite(int fd, const void* buffer,
                                        size_t length, off_t offset) {
  if ((!buffer && length != 0) || offset < 0) {
    return fail(!buffer && length != 0 ? EFAULT : EINVAL,
                static_cast<ssize_t>(-1));
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<ssize_t>(-1));
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<ssize_t>(-1));
  }
  auto result = run(adapter, slot->file.write_at(
                                 static_cast<std::uint64_t>(offset),
                                 {static_cast<const std::byte*>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" off_t orchfs_async_lseek(int fd, off_t offset, int whence) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, static_cast<off_t>(-1));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<off_t>(-1));
  }
  std::unique_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, static_cast<off_t>(-1));
  }
  auto result = run(adapter, slot->file.seek(offset, whence));
  if (!result) {
    return fail(result.error(), static_cast<off_t>(-1));
  }
  if (result.value() >
      static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    return fail(EOVERFLOW, static_cast<off_t>(-1));
  }
  return static_cast<off_t>(result.value());
}

extern "C" int orchfs_async_fstat(int fd, struct stat* stat_buffer) {
  if (!stat_buffer) {
    return fail(EFAULT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, -1);
  }
  auto result = run(adapter, slot->file.stat());
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_stat(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_stat(const char* path,
                                  struct stat* stat_buffer) {
  if (!valid_path(path) || !stat_buffer) {
    return fail(!path || !stat_buffer ? EFAULT : ENOENT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->stat(path));
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_stat(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_fstatfs(int fd, struct statfs* stat_buffer) {
  if (!stat_buffer) {
    return fail(EFAULT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, -1);
  }
  auto result = run(adapter, slot->file.statfs());
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_statfs(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_statfs(const char* path,
                                    struct statfs* stat_buffer) {
  if (!valid_path(path) || !stat_buffer) {
    return fail(!path || !stat_buffer ? EFAULT : ENOENT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto opened = run(adapter, adapter.client->open(std::string(path), O_RDONLY));
  if (!opened) {
    return fail(opened.error(), -1);
  }
  File file = std::move(opened).value();
  auto result = run(adapter, file.statfs());
  const std::error_code operation_error =
      result ? std::error_code{} : result.error();
  (void)run(adapter, file.close());
  if (operation_error) {
    return fail(operation_error, -1);
  }
  populate_statfs(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_truncate(const char* path, off_t length) {
  if (!valid_path(path) || length < 0) {
    return fail(!path ? EFAULT : EINVAL, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->truncate(
                                 path, static_cast<std::uint64_t>(length)));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_ftruncate(int fd, off_t length) {
  if (length < 0) {
    return fail(EINVAL, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, -1);
  }
  auto result =
      run(adapter, slot->file.truncate(static_cast<std::uint64_t>(length)));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_fsync(int fd) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  std::shared_lock lock(slot->mutex);
  if (slot->closed) {
    return fail(EBADF, -1);
  }
  auto result = run(adapter, slot->file.sync());
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_fcntl(int fd, int command, ...) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_file(adapter, fd);
  if (!slot) {
    return fail(EBADF, -1);
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  if (command == F_GETFD || command == F_GETFL) {
    std::shared_lock lock(slot->mutex);
    if (slot->closed) {
      return fail(EBADF, -1);
    }
    if (command == F_GETFD) {
      return slot->descriptor_flags;
    }
    auto result = run(adapter, slot->file.get_flags());
    return result ? result.value() : fail(result.error(), -1);
  }

  if (command == F_SETFD || command == F_SETFL) {
    std::unique_lock lock(slot->mutex);
    if (slot->closed) {
      return fail(EBADF, -1);
    }
    if (command == F_SETFD) {
      va_list arguments;
      va_start(arguments, command);
      slot->descriptor_flags = va_arg(arguments, int) & FD_CLOEXEC;
      va_end(arguments);
      return 0;
    }
    va_list arguments;
    va_start(arguments, command);
    const int flags = va_arg(arguments, int);
    va_end(arguments);
    auto result = run(adapter, slot->file.set_flags(flags));
    if (result) {
      slot->open_flags =
          (slot->open_flags & (O_ACCMODE | O_DIRECTORY)) | flags;
      return 0;
    }
    return fail(result.error(), -1);
  }
#ifdef F_SET_RW_HINT
  if (command == F_SET_RW_HINT) {
    // The legacy wrapper accepted this advisory hint without persisting it.
    return 0;
  }
#endif
  return fail(EOPNOTSUPP, -1);
}

extern "C" int orchfs_async_access(const char* path, int mode) {
  struct stat attributes {};
  if (orchfs_async_stat(path, &attributes) != 0) {
    return -1;
  }
  if (mode == F_OK) {
    return 0;
  }
  mode_t permissions;
  const uid_t user = geteuid();
  if (user == 0) {
    if ((mode & X_OK) != 0 &&
        (attributes.st_mode & (S_IXUSR | S_IXGRP | S_IXOTH)) == 0) {
      return fail(EACCES, -1);
    }
    return 0;
  }
  if (user == attributes.st_uid) {
    permissions = (attributes.st_mode >> 6) & 07;
  } else if (getegid() == attributes.st_gid) {
    permissions = (attributes.st_mode >> 3) & 07;
  } else {
    permissions = attributes.st_mode & 07;
  }
  if (((mode & R_OK) != 0 && (permissions & 04) == 0) ||
      ((mode & W_OK) != 0 && (permissions & 02) == 0) ||
      ((mode & X_OK) != 0 && (permissions & 01) == 0)) {
    return fail(EACCES, -1);
  }
  return 0;
}

extern "C" int orchfs_async_mkdir(const char* path, mode_t mode) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->make_directory(path, mode));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_rmdir(const char* path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->remove_directory(path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_unlink(const char* path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->unlink(path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_rename(const char* old_path,
                                    const char* new_path) {
  if (!valid_path(old_path) || !valid_path(new_path)) {
    return fail(!old_path || !new_path ? EFAULT : ENOENT, -1);
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, -1);
  }
  auto result = run(adapter, adapter.client->rename(old_path, new_path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" DIR* orchfs_async_opendir(const char* path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, static_cast<DIR*>(nullptr));
  }
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!ready(adapter)) {
    return fail(adapter.initialization_error, static_cast<DIR*>(nullptr));
  }
  auto opened = run(adapter, adapter.client->open_directory(path));
  if (!opened) {
    return fail(opened.error(), static_cast<DIR*>(nullptr));
  }
  try {
    auto slot = std::make_shared<DirectorySlot>(std::move(opened).value());
    auto token = std::make_unique<DirectoryToken>();
    DIR* key = reinterpret_cast<DIR*>(token.get());
    {
      std::lock_guard lock(adapter.directories_mutex);
      adapter.directories.emplace(key, std::move(slot));
    }
    (void)token.release();
    return key;
  } catch (const std::bad_alloc&) {
    return fail(ENOMEM, static_cast<DIR*>(nullptr));
  } catch (...) {
    return fail(EIO, static_cast<DIR*>(nullptr));
  }
}

extern "C" int orchfs_async_is_directory(DIR* directory) {
  if (!directory) {
    return 0;
  }
  // DIR is opaque, but every valid libc DIR object has readable object
  // storage. Copying its first bytes as character representation is safe and
  // avoids both dereferencing it as our type and a permanent token registry.
  // Inherited tokens retain the marker after fork and therefore stay on the
  // adapter path, where the new process-local map rejects them with EBADF.
  std::uint64_t magic = 0;
  std::memcpy(&magic, directory, sizeof(magic));
  return magic == kDirectoryTokenMagic ? 1 : 0;
}

extern "C" struct dirent* orchfs_async_readdir(DIR* directory) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!find_directory(adapter, directory)) {
    return fail(EBADF, static_cast<struct dirent*>(nullptr));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error,
                static_cast<struct dirent*>(nullptr));
  }
  return read_directory_entry(adapter, directory, &DirectorySlot::entry);
}

extern "C" struct dirent64* orchfs_async_readdir64(DIR* directory) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  if (!find_directory(adapter, directory)) {
    return fail(EBADF, static_cast<struct dirent64*>(nullptr));
  }
  if (!ready(adapter)) {
    return fail(adapter.initialization_error,
                static_cast<struct dirent64*>(nullptr));
  }
  return read_directory_entry(adapter, directory, &DirectorySlot::entry64);
}

extern "C" int orchfs_async_closedir(DIR* directory) {
  auto& adapter = state();
  std::shared_lock lifecycle_lock(adapter.lifecycle_mutex);
  auto slot = find_directory(adapter, directory);
  if (!slot) {
    return fail(EBADF, -1);
  }
  {
    std::lock_guard lock(slot->mutex);
    if (slot->closed) {
      return fail(EBADF, -1);
    }
    if (ready(adapter)) {
      auto result = run(adapter, slot->directory.close());
      if (!result) {
        return fail(result.error(), -1);
      }
    }
    slot->closed = true;
  }
  {
    std::lock_guard lock(adapter.directories_mutex);
    const auto found = adapter.directories.find(directory);
    if (found != adapter.directories.end() && found->second == slot) {
      adapter.directories.erase(found);
    }
  }
  auto* token = reinterpret_cast<DirectoryToken*>(directory);
  if (token->magic == kDirectoryTokenMagic) {
    token->magic = 0;
  }
  delete token;
  return 0;
}
