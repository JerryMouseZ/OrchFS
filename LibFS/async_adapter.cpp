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
#include <climits>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <sched.h>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

namespace {

using orchfs::async::Client;
using orchfs::async::ClientOptions;
using orchfs::async::Directory;
using orchfs::async::DirEntry;
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

// Application threads may wait for a POSIX call, but they must never create
// or depend on a service thread.  This gate is used only at the synchronous
// ABI boundary to hand ownership of a small piece of adapter state to one
// caller at a time.  The actual filesystem operation is always submitted to
// the session's fixed Runtime owner.
class OwnerGate {
public:
  OwnerGate() = default;
  OwnerGate(const OwnerGate &) = delete;
  OwnerGate &operator=(const OwnerGate &) = delete;

  void acquire() noexcept {
    bool expected = false;
    while (!occupied_.compare_exchange_weak(
        expected, true, std::memory_order_acquire, std::memory_order_relaxed)) {
      expected = true;
      occupied_.wait(true, std::memory_order_relaxed);
      expected = false;
    }
  }

  void release() noexcept {
    occupied_.store(false, std::memory_order_release);
    occupied_.notify_one();
  }

private:
  std::atomic<bool> occupied_{false};
};

class OwnerLease {
public:
  explicit OwnerLease(OwnerGate &gate) noexcept : gate_(&gate) {
    gate_->acquire();
  }
  ~OwnerLease() {
    if (gate_) {
      gate_->release();
    }
  }
  OwnerLease(const OwnerLease &) = delete;
  OwnerLease &operator=(const OwnerLease &) = delete;
  OwnerLease(OwnerLease &&other) noexcept
      : gate_(std::exchange(other.gate_, nullptr)) {}

private:
  OwnerGate *gate_;
};

enum class SlotState : std::uint8_t { open, closing, closed };

struct SessionEpoch {
  SessionEpoch(Runtime &runtime_value, Client client_value,
               std::uint64_t generation_value) noexcept
      : runtime(&runtime_value), client(std::move(client_value)),
        generation(generation_value) {}

  Runtime *runtime;
  Client client;
  std::uint64_t generation;
};

struct FileSlot {
  FileSlot(File value, std::optional<Directory> directory_value,
           std::string opened_path, int flags,
           std::shared_ptr<SessionEpoch> epoch_value)
      : epoch(std::move(epoch_value)), file(std::move(value)),
        directory(std::move(directory_value)),
        directory_checked(directory.has_value()), path(std::move(opened_path)),
        open_flags(flags),
        descriptor_flags((flags & O_CLOEXEC) != 0 ? FD_CLOEXEC : 0) {}

  OwnerGate owner;
  std::shared_ptr<SessionEpoch> epoch;
  std::atomic<std::size_t> inflight{0};
  File file;
  std::optional<Directory> directory;
  bool directory_checked{};
  std::string path;
  int open_flags{};
  int descriptor_flags{};
  SlotState state{SlotState::open};
};

struct DirectoryToken {
  std::uint64_t magic{kDirectoryTokenMagic};
  pid_t owner_pid{::getpid()};
};

struct DirectorySlot {
  DirectorySlot(Directory value, std::shared_ptr<SessionEpoch> epoch_value)
      : epoch(std::move(epoch_value)), directory(std::move(value)) {}

  OwnerGate owner;
  std::shared_ptr<SessionEpoch> epoch;
  std::atomic<std::size_t> inflight{0};
  Directory directory;
  std::array<DirEntry, kDirectoryBatchSize> batch;
  std::size_t cursor{};
  std::size_t count{};
  SlotState state{SlotState::open};
  struct dirent entry {};
  struct dirent64 entry64 {};
};

using FileTable = std::unordered_map<int, std::shared_ptr<FileSlot>>;
using DirectoryTable =
    std::unordered_map<DIR *, std::shared_ptr<DirectorySlot>>;

struct AdapterState {
  pid_t owner_pid{::getpid()};
  OwnerGate lifecycle_owner;
  std::unique_ptr<Runtime> runtime;
  std::shared_ptr<SessionEpoch> current_epoch;
  std::uint64_t next_epoch{1};
  std::error_code initialization_error;
  bool accepting_calls{true};
  std::atomic<std::size_t> active_calls{0};
  std::atomic<std::size_t> peak_active_calls{0};
  std::atomic<bool> ever_connected{false};

  std::atomic<std::shared_ptr<const FileTable>> files{
      std::make_shared<const FileTable>()};
  std::atomic<std::uint64_t> next_fd{3};

  std::atomic<std::shared_ptr<const DirectoryTable>> directories{
      std::make_shared<const DirectoryTable>()};
};

AdapterState &state() {
  // The LD_PRELOAD destructor performs the ordered teardown.  Keeping the
  // state object itself alive avoids static-destruction-order races with libc.
  static std::atomic<AdapterState *> value{new AdapterState};
  const pid_t process = ::getpid();
  for (;;) {
    AdapterState *current = value.load(std::memory_order_acquire);
    if (current->owner_pid == process) {
      return *current;
    }

    // Only the calling thread survives fork(). Parent Runtime threads and any
    // Runtime ownership state cannot be joined or safely destroyed in the
    // child, so abandon that process-local state and lazily establish a fresh
    // session. CAS also handles multiple child threads making their first
    // adapter call concurrently without racing on the process-local state
    // pointer.
    auto *replacement = new AdapterState;
    replacement->ever_connected.store(
        current->ever_connected.load(std::memory_order_acquire),
        std::memory_order_relaxed);
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

void begin_active_call(AdapterState &adapter) noexcept {
  const std::size_t active =
      adapter.active_calls.fetch_add(1, std::memory_order_acq_rel) + 1;
  std::size_t peak =
      adapter.peak_active_calls.load(std::memory_order_relaxed);
  while (peak < active &&
         !adapter.peak_active_calls.compare_exchange_weak(
             peak, active, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
}

void finish_active_call(AdapterState &adapter) noexcept {
  if (adapter.active_calls.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    adapter.active_calls.notify_all();
  }
}

void wait_for_active_calls(AdapterState &adapter) noexcept {
  std::size_t active = adapter.active_calls.load(std::memory_order_acquire);
  while (active != 0) {
    adapter.active_calls.wait(active, std::memory_order_relaxed);
    active = adapter.active_calls.load(std::memory_order_acquire);
  }
}

template <typename Slot>
void finish_slot_call(AdapterState &adapter, Slot &slot) noexcept {
  if (slot.inflight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    slot.inflight.notify_all();
  }
  finish_active_call(adapter);
}

template <typename Slot>
void wait_for_slot_calls(Slot &slot) noexcept {
  std::size_t active = slot.inflight.load(std::memory_order_acquire);
  while (active != 0) {
    slot.inflight.wait(active, std::memory_order_relaxed);
    active = slot.inflight.load(std::memory_order_acquire);
  }
}

class EpochLease {
public:
  EpochLease(AdapterState &adapter,
             std::shared_ptr<SessionEpoch> epoch) noexcept
      : adapter_(&adapter), epoch_(std::move(epoch)) {}
  ~EpochLease() {
    if (adapter_) {
      finish_active_call(*adapter_);
    }
  }
  EpochLease(const EpochLease &) = delete;
  EpochLease &operator=(const EpochLease &) = delete;
  EpochLease(EpochLease &&other) noexcept
      : adapter_(std::exchange(other.adapter_, nullptr)),
        epoch_(std::move(other.epoch_)) {}

  SessionEpoch &epoch() const noexcept { return *epoch_; }
  const std::shared_ptr<SessionEpoch> &shared_epoch() const noexcept {
    return epoch_;
  }

private:
  AdapterState *adapter_;
  std::shared_ptr<SessionEpoch> epoch_;
};

template <typename Slot>
class SlotLease {
public:
  SlotLease(AdapterState &adapter, std::shared_ptr<Slot> slot) noexcept
      : adapter_(&adapter), slot_(std::move(slot)), epoch_(slot_->epoch) {}
  ~SlotLease() {
    if (adapter_) {
      finish_slot_call(*adapter_, *slot_);
    }
  }
  SlotLease(const SlotLease &) = delete;
  SlotLease &operator=(const SlotLease &) = delete;
  SlotLease(SlotLease &&other) noexcept
      : adapter_(std::exchange(other.adapter_, nullptr)),
        slot_(std::move(other.slot_)), epoch_(std::move(other.epoch_)) {}

  Slot &slot() const noexcept { return *slot_; }
  SessionEpoch &epoch() const noexcept { return *epoch_; }
  const std::shared_ptr<Slot> &shared_slot() const noexcept { return slot_; }
  const std::shared_ptr<SessionEpoch> &shared_epoch() const noexcept {
    return epoch_;
  }

private:
  AdapterState *adapter_;
  std::shared_ptr<Slot> slot_;
  std::shared_ptr<SessionEpoch> epoch_;
};

using FileLease = SlotLease<FileSlot>;
using DirectoryLease = SlotLease<DirectorySlot>;

template <typename T>
Result<T> run(Runtime &runtime, Task<Result<T>> task) {
  auto completed = runtime.block_on(std::move(task));
  if (!completed) {
    return Result<T>::failure(completed.error());
  }
  return std::move(completed).value();
}

template <typename T>
Result<T> run(SessionEpoch &epoch, Task<Result<T>> task) {
  return run(*epoch.runtime, std::move(task));
}

bool ready_locked(const AdapterState &adapter) noexcept {
  return adapter.accepting_calls && adapter.runtime && adapter.current_epoch &&
         adapter.current_epoch->client.valid();
}

std::error_code unavailable_error_locked(const AdapterState &adapter) noexcept {
  if (!adapter.accepting_calls) {
    return orchfs::async::make_error_code(Errc::runtime_stopping);
  }
  return adapter.initialization_error
             ? adapter.initialization_error
             : std::make_error_code(std::errc::not_connected);
}

std::optional<EpochLease> acquire_epoch(AdapterState &adapter) noexcept {
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  if (!ready_locked(adapter)) {
    errno = errno_from_error(unavailable_error_locked(adapter));
    return std::nullopt;
  }
  begin_active_call(adapter);
  return std::optional<EpochLease>(
      std::in_place, adapter, adapter.current_epoch);
}

std::size_t configured_worker_count() noexcept {
  // POSIX callers already provide request concurrency.  One Runtime worker
  // keeps the SHM lane hot without spending the remaining client CPUs in
  // duplicate busy-poll loops; deployments can still opt into more workers.
  constexpr std::size_t kDefaultWorkerCount = 1;
  const char *text = std::getenv("ORCHFS_CLIENT_WORKERS");
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
  char *end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    return kDefaultWorkerCount;
  }
  return static_cast<std::size_t>(parsed);
}

std::size_t configured_size(const char *name, std::size_t fallback) noexcept {
  const char *text = std::getenv(name);
  if (!text || !*text) {
    return fallback;
  }
  char *end = nullptr;
  errno = 0;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || parsed == 0 ||
      parsed > std::numeric_limits<std::size_t>::max()) {
    return fallback;
  }
  return static_cast<std::size_t>(parsed);
}

std::shared_ptr<FileSlot> find_file(AdapterState &adapter, int fd) {
  const auto snapshot = adapter.files.load(std::memory_order_acquire);
  const auto found = snapshot->find(fd);
  return found == snapshot->end() ? nullptr : found->second;
}

int insert_file(AdapterState &adapter, std::shared_ptr<FileSlot> slot) {
  for (int attempts = 0; attempts < kMaximumLocalFd; ++attempts) {
    const std::uint64_t sequence =
        adapter.next_fd.fetch_add(1, std::memory_order_relaxed);
    const int candidate =
        3 + static_cast<int>((sequence - 3) % (kMaximumLocalFd - 2));
    auto current = adapter.files.load(std::memory_order_acquire);
    if (current->size() >= static_cast<std::size_t>(kMaximumLocalFd - 2)) {
      return -1;
    }
    if (current->contains(candidate)) {
      continue;
    }
    auto replacement = std::make_shared<FileTable>(*current);
    replacement->emplace(candidate, slot);
    std::shared_ptr<const FileTable> published = std::move(replacement);
    if (adapter.files.compare_exchange_weak(current, std::move(published),
                                            std::memory_order_release,
                                            std::memory_order_acquire)) {
      return candidate;
    }
  }
  return -1;
}

void erase_file(AdapterState &adapter, int fd,
                const std::shared_ptr<FileSlot> &slot) {
  auto current = adapter.files.load(std::memory_order_acquire);
  for (;;) {
    const auto found = current->find(fd);
    if (found == current->end() || found->second != slot) {
      return;
    }
    auto replacement = std::make_shared<FileTable>(*current);
    replacement->erase(fd);
    std::shared_ptr<const FileTable> published = std::move(replacement);
    if (adapter.files.compare_exchange_weak(current, std::move(published),
                                            std::memory_order_release,
                                            std::memory_order_acquire)) {
      return;
    }
  }
}

std::shared_ptr<DirectorySlot> find_directory(AdapterState &adapter,
                                              DIR *directory) {
  const auto snapshot = adapter.directories.load(std::memory_order_acquire);
  const auto found = snapshot->find(directory);
  return found == snapshot->end() ? nullptr : found->second;
}

void insert_directory(AdapterState &adapter, DIR *key,
                      std::shared_ptr<DirectorySlot> slot) {
  auto current = adapter.directories.load(std::memory_order_acquire);
  for (;;) {
    auto replacement = std::make_shared<DirectoryTable>(*current);
    replacement->emplace(key, slot);
    std::shared_ptr<const DirectoryTable> published = std::move(replacement);
    if (adapter.directories.compare_exchange_weak(current, std::move(published),
                                                  std::memory_order_release,
                                                  std::memory_order_acquire)) {
      return;
    }
  }
}

void erase_directory(AdapterState &adapter, DIR *key,
                     const std::shared_ptr<DirectorySlot> &slot) {
  auto current = adapter.directories.load(std::memory_order_acquire);
  for (;;) {
    const auto found = current->find(key);
    if (found == current->end() || found->second != slot) {
      return;
    }
    auto replacement = std::make_shared<DirectoryTable>(*current);
    replacement->erase(key);
    std::shared_ptr<const DirectoryTable> published = std::move(replacement);
    if (adapter.directories.compare_exchange_weak(current, std::move(published),
                                                  std::memory_order_release,
                                                  std::memory_order_acquire)) {
      return;
    }
  }
}

std::optional<FileLease> acquire_file(AdapterState &adapter, int fd) noexcept {
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  if (!adapter.accepting_calls) {
    errno = ESHUTDOWN;
    return std::nullopt;
  }
  auto slot = find_file(adapter, fd);
  if (!slot) {
    errno = EBADF;
    return std::nullopt;
  }
  OwnerLease slot_lock(slot->owner);
  if (slot->state != SlotState::open) {
    errno = EBADF;
    return std::nullopt;
  }
  if (!slot->epoch || !slot->epoch->client.valid()) {
    errno = errno_from_error(unavailable_error_locked(adapter));
    return std::nullopt;
  }
  slot->inflight.fetch_add(1, std::memory_order_acq_rel);
  begin_active_call(adapter);
  return std::optional<FileLease>(std::in_place, adapter, std::move(slot));
}

std::optional<DirectoryLease>
acquire_directory(AdapterState &adapter, DIR *directory) noexcept {
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  if (!adapter.accepting_calls) {
    errno = ESHUTDOWN;
    return std::nullopt;
  }
  auto slot = find_directory(adapter, directory);
  if (!slot) {
    errno = EBADF;
    return std::nullopt;
  }
  OwnerLease slot_lock(slot->owner);
  if (slot->state != SlotState::open) {
    errno = EBADF;
    return std::nullopt;
  }
  if (!slot->epoch || !slot->epoch->client.valid()) {
    errno = errno_from_error(unavailable_error_locked(adapter));
    return std::nullopt;
  }
  slot->inflight.fetch_add(1, std::memory_order_acq_rel);
  begin_active_call(adapter);
  return std::optional<DirectoryLease>(std::in_place, adapter,
                                       std::move(slot));
}

void populate_stat(const FileStat &input, struct stat &output) noexcept {
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

void populate_statfs(const FileSystemStat &input,
                     struct statfs &output) noexcept {
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

bool valid_path(const char *path) noexcept {
  return path != nullptr && *path != '\0';
}

std::optional<std::string> joined_openat_path(const FileSlot &directory,
                                              const char *path) {
  try {
    return (std::filesystem::path(directory.path) / path)
        .lexically_normal()
        .generic_string();
  } catch (const std::bad_alloc &) {
    errno = ENOMEM;
    return std::nullopt;
  } catch (...) {
    errno = EINVAL;
    return std::nullopt;
  }
}

std::error_code ensure_directory_handle(SessionEpoch &epoch, FileSlot &slot) {
  if (slot.directory) {
    return {};
  }
  if (slot.directory_checked) {
    return std::make_error_code(std::errc::not_a_directory);
  }

  auto opened = run(epoch, epoch.client.open_directory(slot.file));
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

std::optional<std::string> path_for_openat(FileLease &directory,
                                           const char *path) {
  if (!valid_path(path)) {
    errno = path ? ENOENT : EFAULT;
    return std::nullopt;
  }

  // Resolve the capability lazily from the already-open File handle. This
  // preserves the inode selected by open(2), even if its pathname is renamed
  // or recreated before the first openat(2), without taxing ordinary file
  // opens with a second RPC.
  auto &slot = directory.slot();
  OwnerLease lock(slot.owner);
  if (const auto error = ensure_directory_handle(directory.epoch(), slot);
      error) {
    errno = errno_from_error(error);
    return std::nullopt;
  }
  return joined_openat_path(slot, path);
}

template <typename Entry>
void populate_directory_entry(const DirEntry &input, Entry &output) noexcept {
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
Entry *read_directory_entry(DirectoryLease &lease,
                            Entry DirectorySlot::*entry_member) {
  auto &slot = lease.slot();
  OwnerLease lock(slot.owner);
  if (slot.cursor == slot.count) {
    slot.cursor = 0;
    auto result = run(lease.epoch(), slot.directory.next_batch(slot.batch));
    if (!result) {
      return fail(result.error(), static_cast<Entry *>(nullptr));
    }
    slot.count = std::move(result).value();
    if (slot.count == 0) {
      errno = 0;
      return nullptr;
    }
  }
  Entry &output = slot.*entry_member;
  populate_directory_entry(slot.batch[slot.cursor++], output);
  return &output;
}

int install_opened_file(AdapterState &adapter, Result<File> opened,
                        std::string path, int flags,
                        std::shared_ptr<SessionEpoch> epoch) {
  if (!opened) {
    return fail(opened.error(), -1);
  }
  try {
    auto slot = std::make_shared<FileSlot>(
        std::move(opened).value(), std::nullopt, std::move(path), flags,
        std::move(epoch));
    const int fd = insert_file(adapter, std::move(slot));
    if (fd < 0) {
      return fail(EMFILE, -1);
    }
    return fd;
  } catch (const std::bad_alloc &) {
    return fail(ENOMEM, -1);
  } catch (...) {
    return fail(EIO, -1);
  }
}

} // namespace

extern "C" int orchfs_async_adapter_init(void) {
  auto &adapter = state();
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  if (!adapter.accepting_calls) {
    return fail(ESHUTDOWN, -1);
  }
  if (ready_locked(adapter)) {
    return 0;
  }

  bool created_runtime = false;
  try {
    if (!adapter.runtime) {
      RuntimeOptions runtime_options;
      runtime_options.worker_count = configured_worker_count();
      runtime_options.blocking_spin_count =
          configured_size("ORCHFS_CLIENT_BLOCKING_SPINS", 1024);
      runtime_options.blocking_spin_limit =
          configured_size("ORCHFS_CLIENT_BLOCKING_SPIN_LIMIT", 4);
      runtime_options.blocking_spin_warmup =
          configured_size("ORCHFS_CLIENT_BLOCKING_SPIN_WARMUP", 64);
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
      adapter.current_epoch.reset();
    }

    ClientOptions client_options;
    if (const char *endpoint = std::getenv("ORCHFS_ASYNC_ENDPOINT");
        endpoint && *endpoint) {
      client_options.endpoint = endpoint;
    }
    client_options.lane_count =
        configured_size("ORCHFS_CLIENT_LANES", 4);
    client_options.ring_capacity = configured_size(
        "ORCHFS_IPC_RING_CAPACITY", client_options.ring_capacity);
    client_options.data_slot_size = configured_size(
        "ORCHFS_IPC_DATA_SLOT_SIZE", client_options.data_slot_size);
    auto connected = run(
        *adapter.runtime,
        Client::connect(*adapter.runtime, std::move(client_options)));
    if (!connected) {
      adapter.initialization_error = connected.error();
      if (created_runtime) {
        adapter.runtime->request_stop();
        (void)adapter.runtime->join();
        adapter.runtime.reset();
      }
      return fail(adapter.initialization_error, -1);
    }
    adapter.current_epoch = std::make_shared<SessionEpoch>(
        *adapter.runtime, std::move(connected).value(), adapter.next_epoch++);
    adapter.ever_connected.store(true, std::memory_order_release);
    adapter.initialization_error.clear();
    return 0;
  } catch (const std::bad_alloc &) {
    adapter.initialization_error =
        std::make_error_code(std::errc::not_enough_memory);
  } catch (...) {
    adapter.initialization_error = std::make_error_code(std::errc::io_error);
  }

  adapter.current_epoch.reset();
  if (created_runtime && adapter.runtime) {
    adapter.runtime->request_stop();
    (void)adapter.runtime->join();
    adapter.runtime.reset();
  }
  return fail(adapter.initialization_error, -1);
}

extern "C" int orchfs_async_adapter_ready(void) {
  auto &adapter = state();
  {
    OwnerLease lifecycle_lock(adapter.lifecycle_owner);
    if (ready_locked(adapter)) {
      return 1;
    }
    if (!adapter.accepting_calls) {
      errno = ESHUTDOWN;
      return 0;
    }
  }
  // Pathname wrappers use this as the lazy reconnect point. init() serializes
  // concurrent callers and reuses the process Runtime after a dead session.
  return orchfs_async_adapter_init() == 0 ? 1 : 0;
}

extern "C" size_t orchfs_async_adapter_peak_inflight(void) {
  return state().peak_active_calls.load(std::memory_order_acquire);
}

extern "C" int orchfs_async_adapter_allow_host_fallback(int adapter_was_ready,
                                                        int error_number) {
  auto &adapter = state();
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  if (adapter_was_ready) {
    // The operation reached a live session, so ENOENT is a filesystem lookup
    // result rather than a missing control socket from connect(2).
    return error_number == ENOENT;
  }
  return !adapter.ever_connected.load(std::memory_order_acquire);
}

extern "C" void orchfs_async_adapter_shutdown(void) {
  auto &adapter = state();
  OwnerLease lifecycle_lock(adapter.lifecycle_owner);
  adapter.accepting_calls = false;
  wait_for_active_calls(adapter);
  if (!adapter.runtime) {
    adapter.current_epoch.reset();
    return;
  }

  auto files = adapter.files.exchange(std::make_shared<const FileTable>(),
                                      std::memory_order_acq_rel);
  for (const auto &[fd, slot] : *files) {
    (void)fd;
    OwnerLease lock(slot->owner);
    slot->state = SlotState::closing;
    wait_for_slot_calls(*slot);
    if (slot->epoch) {
      (void)run(*slot->epoch, slot->file.close());
    }
    if (slot->directory) {
      if (slot->epoch) {
        (void)run(*slot->epoch, slot->directory->close());
      }
      slot->directory.reset();
    }
    slot->state = SlotState::closed;
  }

  auto directories = adapter.directories.exchange(
      std::make_shared<const DirectoryTable>(), std::memory_order_acq_rel);
  for (const auto &[token, slot] : *directories) {
    {
      OwnerLease lock(slot->owner);
      slot->state = SlotState::closing;
      wait_for_slot_calls(*slot);
      if (slot->epoch) {
        (void)run(*slot->epoch, slot->directory.close());
      }
      slot->state = SlotState::closed;
    }
    delete reinterpret_cast<DirectoryToken *>(token);
  }

  // Destroy every slot while Runtime is still alive. A failed remote close
  // leaves RAII handles that perform best-effort detached cleanup from their
  // destructors; letting these maps outlive runtime.reset() would leave their
  // Session::runtime_ pointers dangling.
  files.reset();
  directories.reset();

  if (adapter.current_epoch && adapter.current_epoch->client.valid()) {
    (void)run(*adapter.current_epoch,
              adapter.current_epoch->client.shutdown());
  }
  adapter.current_epoch.reset();
  adapter.runtime->request_stop();
  (void)adapter.runtime->join();
  adapter.runtime.reset();
}

extern "C" int orchfs_async_open(const char *path, int flags, ...) {
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

  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto opened = run(epoch->epoch(),
                    epoch->epoch().client.open(path, flags, mode));
  return install_opened_file(adapter, std::move(opened), std::string(path),
                             flags, epoch->shared_epoch());
}

extern "C" int orchfs_async_openat(int dirfd, const char *path, int flags,
                                   ...) {
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

  auto &adapter = state();
  {
    OwnerLease lifecycle_lock(adapter.lifecycle_owner);
    if (!ready_locked(adapter)) {
      return fail(unavailable_error_locked(adapter), -1);
    }
  }
  if (*path == '/' || dirfd == AT_FDCWD) {
    auto epoch = acquire_epoch(adapter);
    if (!epoch) {
      return -1;
    }
    auto opened = run(epoch->epoch(),
                      epoch->epoch().client.open(path, flags, mode));
    return install_opened_file(adapter, std::move(opened), std::string(path),
                               flags, epoch->shared_epoch());
  }

  auto parent = acquire_file(adapter, dirfd);
  if (!parent) {
    return -1;
  }
  auto combined_path = path_for_openat(*parent, path);
  if (!combined_path) {
    return -1;
  }

  Result<File> opened = [&]() {
    OwnerLease parent_lock(parent->slot().owner);
    if (!parent->slot().directory) {
      return Result<File>::failure(
          std::make_error_code(std::errc::not_a_directory));
    }
    return run(parent->epoch(),
               parent->epoch().client.open_at(*parent->slot().directory,
                                              std::string(path), flags, mode));
  }();
  return install_opened_file(adapter, std::move(opened),
                             std::move(*combined_path), flags,
                             parent->shared_epoch());
}

extern "C" int orchfs_async_close(int fd) {
  auto &adapter = state();
  std::shared_ptr<FileSlot> slot;
  std::optional<EpochLease> call;
  {
    OwnerLease lifecycle_lock(adapter.lifecycle_owner);
    if (!adapter.accepting_calls) {
      return fail(ESHUTDOWN, -1);
    }
    slot = find_file(adapter, fd);
    if (!slot) {
      return fail(EBADF, -1);
    }
    OwnerLease slot_lock(slot->owner);
    if (slot->state != SlotState::open) {
      return fail(EBADF, -1);
    }
    slot->state = SlotState::closing;
    begin_active_call(adapter);
    call.emplace(adapter, slot->epoch);
  }

  wait_for_slot_calls(*slot);
  if (!call->epoch().client.valid()) {
    // A dead transport makes a remote retry impossible; the server's
    // once-only session cleanup owns the peer handle. Closing the local proxy
    // must still retire it instead of leaking it until a pathname reconnect.
    {
      OwnerLease slot_lock(slot->owner);
      slot->state = SlotState::closed;
      slot->directory.reset();
    }
    erase_file(adapter, fd, slot);
    return 0;
  }
  auto result = run(call->epoch(), slot->file.close());
  if (!result) {
    if (!call->epoch().client.valid()) {
      {
        OwnerLease slot_lock(slot->owner);
        slot->state = SlotState::closed;
        slot->directory.reset();
      }
      erase_file(adapter, fd, slot);
      return 0;
    }
    // close(2) failures leave this compatibility handle usable so callers can
    // observe the error and retry.  No local state is discarded first.
    {
      OwnerLease slot_lock(slot->owner);
      slot->state = SlotState::open;
    }
    return fail(result.error(), -1);
  }
  {
    OwnerLease slot_lock(slot->owner);
    if (slot->directory) {
      // This is an auxiliary handle used solely to preserve openat inode
      // semantics.  The user-visible File close already succeeded.
      (void)run(call->epoch(), slot->directory->close());
      slot->directory.reset();
    }
    slot->state = SlotState::closed;
  }
  erase_file(adapter, fd, slot);
  return 0;
}

extern "C" ssize_t orchfs_async_read(int fd, void *buffer, size_t length) {
  if (!buffer && length != 0) {
    return fail(EFAULT, static_cast<ssize_t>(-1));
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(
      lease->epoch(),
      lease->slot().file.read({static_cast<std::byte *>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_write(int fd, const void *buffer,
                                      size_t length) {
  if (!buffer && length != 0) {
    return fail(EFAULT, static_cast<ssize_t>(-1));
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result =
      run(lease->epoch(), lease->slot().file.write(
                              {static_cast<const std::byte *>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_pread(int fd, void *buffer, size_t length,
                                      off_t offset) {
  if ((!buffer && length != 0) || offset < 0) {
    return fail(!buffer && length != 0 ? EFAULT : EINVAL,
                static_cast<ssize_t>(-1));
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(
      lease->epoch(),
      lease->slot().file.read_at(static_cast<std::uint64_t>(offset),
                                 {static_cast<std::byte *>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" ssize_t orchfs_async_pwrite(int fd, const void *buffer,
                                       size_t length, off_t offset) {
  if ((!buffer && length != 0) || offset < 0) {
    return fail(!buffer && length != 0 ? EFAULT : EINVAL,
                static_cast<ssize_t>(-1));
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(
      lease->epoch(),
      lease->slot().file.write_at(
          static_cast<std::uint64_t>(offset),
          {static_cast<const std::byte *>(buffer), length}));
  if (!result) {
    return fail(result.error(), static_cast<ssize_t>(-1));
  }
  if (result.value() > static_cast<std::size_t>(SSIZE_MAX)) {
    return fail(EOVERFLOW, static_cast<ssize_t>(-1));
  }
  return static_cast<ssize_t>(result.value());
}

extern "C" off_t orchfs_async_lseek(int fd, off_t offset, int whence) {
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(lease->epoch(), lease->slot().file.seek(offset, whence));
  if (!result) {
    return fail(result.error(), static_cast<off_t>(-1));
  }
  if (result.value() >
      static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())) {
    return fail(EOVERFLOW, static_cast<off_t>(-1));
  }
  return static_cast<off_t>(result.value());
}

extern "C" int orchfs_async_fstat(int fd, struct stat *stat_buffer) {
  if (!stat_buffer) {
    return fail(EFAULT, -1);
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(lease->epoch(), lease->slot().file.stat());
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_stat(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_stat(const char *path, struct stat *stat_buffer) {
  if (!valid_path(path) || !stat_buffer) {
    return fail(!path || !stat_buffer ? EFAULT : ENOENT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result = run(epoch->epoch(), epoch->epoch().client.stat(path));
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_stat(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_fstatfs(int fd, struct statfs *stat_buffer) {
  if (!stat_buffer) {
    return fail(EFAULT, -1);
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(lease->epoch(), lease->slot().file.statfs());
  if (!result) {
    return fail(result.error(), -1);
  }
  populate_statfs(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_statfs(const char *path,
                                   struct statfs *stat_buffer) {
  if (!valid_path(path) || !stat_buffer) {
    return fail(!path || !stat_buffer ? EFAULT : ENOENT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto opened = run(epoch->epoch(),
                    epoch->epoch().client.open(std::string(path), O_RDONLY));
  if (!opened) {
    return fail(opened.error(), -1);
  }
  File file = std::move(opened).value();
  auto result = run(epoch->epoch(), file.statfs());
  const std::error_code operation_error =
      result ? std::error_code{} : result.error();
  (void)run(epoch->epoch(), file.close());
  if (operation_error) {
    return fail(operation_error, -1);
  }
  populate_statfs(result.value(), *stat_buffer);
  return 0;
}

extern "C" int orchfs_async_truncate(const char *path, off_t length) {
  if (!valid_path(path) || length < 0) {
    return fail(!path ? EFAULT : EINVAL, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result = run(epoch->epoch(), epoch->epoch().client.truncate(
                                         path, static_cast<std::uint64_t>(length)));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_ftruncate(int fd, off_t length) {
  if (length < 0) {
    return fail(EINVAL, -1);
  }
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(
      lease->epoch(),
      lease->slot().file.truncate(static_cast<std::uint64_t>(length)));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_fsync(int fd) {
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto result = run(lease->epoch(), lease->slot().file.sync());
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_fcntl(int fd, int command, ...) {
  auto &adapter = state();
  auto lease = acquire_file(adapter, fd);
  if (!lease) {
    return -1;
  }
  auto &slot = lease->slot();
  if (command == F_GETFD || command == F_GETFL) {
    OwnerLease lock(slot.owner);
    if (command == F_GETFD) {
      return slot.descriptor_flags;
    }
    auto result = run(lease->epoch(), slot.file.get_flags());
    return result ? result.value() : fail(result.error(), -1);
  }

  if (command == F_SETFD || command == F_SETFL) {
    OwnerLease lock(slot.owner);
    if (command == F_SETFD) {
      va_list arguments;
      va_start(arguments, command);
      slot.descriptor_flags = va_arg(arguments, int) & FD_CLOEXEC;
      va_end(arguments);
      return 0;
    }
    va_list arguments;
    va_start(arguments, command);
    const int flags = va_arg(arguments, int);
    va_end(arguments);
    auto result = run(lease->epoch(), slot.file.set_flags(flags));
    if (result) {
      slot.open_flags = (slot.open_flags & (O_ACCMODE | O_DIRECTORY)) | flags;
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

extern "C" int orchfs_async_access(const char *path, int mode) {
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

extern "C" int orchfs_async_mkdir(const char *path, mode_t mode) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result =
      run(epoch->epoch(), epoch->epoch().client.make_directory(path, mode));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_rmdir(const char *path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result =
      run(epoch->epoch(), epoch->epoch().client.remove_directory(path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_unlink(const char *path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result = run(epoch->epoch(), epoch->epoch().client.unlink(path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" int orchfs_async_rename(const char *old_path, const char *new_path) {
  if (!valid_path(old_path) || !valid_path(new_path)) {
    return fail(!old_path || !new_path ? EFAULT : ENOENT, -1);
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return -1;
  }
  auto result =
      run(epoch->epoch(), epoch->epoch().client.rename(old_path, new_path));
  return result ? 0 : fail(result.error(), -1);
}

extern "C" DIR *orchfs_async_opendir(const char *path) {
  if (!valid_path(path)) {
    return fail(path ? ENOENT : EFAULT, static_cast<DIR *>(nullptr));
  }
  auto &adapter = state();
  auto epoch = acquire_epoch(adapter);
  if (!epoch) {
    return nullptr;
  }
  auto opened =
      run(epoch->epoch(), epoch->epoch().client.open_directory(path));
  if (!opened) {
    return fail(opened.error(), static_cast<DIR *>(nullptr));
  }
  try {
    auto slot = std::make_shared<DirectorySlot>(
        std::move(opened).value(), epoch->shared_epoch());
    auto token = std::make_unique<DirectoryToken>();
    DIR *key = reinterpret_cast<DIR *>(token.get());
    insert_directory(adapter, key, std::move(slot));
    (void)token.release();
    return key;
  } catch (const std::bad_alloc &) {
    return fail(ENOMEM, static_cast<DIR *>(nullptr));
  } catch (...) {
    return fail(EIO, static_cast<DIR *>(nullptr));
  }
}

extern "C" int orchfs_async_is_directory(DIR *directory) {
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

extern "C" struct dirent *orchfs_async_readdir(DIR *directory) {
  auto &adapter = state();
  auto lease = acquire_directory(adapter, directory);
  if (!lease) {
    return nullptr;
  }
  return read_directory_entry(*lease, &DirectorySlot::entry);
}

extern "C" struct dirent64 *orchfs_async_readdir64(DIR *directory) {
  auto &adapter = state();
  auto lease = acquire_directory(adapter, directory);
  if (!lease) {
    return nullptr;
  }
  return read_directory_entry(*lease, &DirectorySlot::entry64);
}

extern "C" int orchfs_async_closedir(DIR *directory) {
  auto &adapter = state();
  std::shared_ptr<DirectorySlot> slot;
  std::optional<EpochLease> call;
  {
    OwnerLease lifecycle_lock(adapter.lifecycle_owner);
    if (!adapter.accepting_calls) {
      return fail(ESHUTDOWN, -1);
    }
    slot = find_directory(adapter, directory);
    if (!slot) {
      return fail(EBADF, -1);
    }
    OwnerLease slot_lock(slot->owner);
    if (slot->state != SlotState::open) {
      return fail(EBADF, -1);
    }
    slot->state = SlotState::closing;
    begin_active_call(adapter);
    call.emplace(adapter, slot->epoch);
  }

  wait_for_slot_calls(*slot);
  if (call->epoch().client.valid()) {
    auto result = run(call->epoch(), slot->directory.close());
    if (!result && call->epoch().client.valid()) {
      OwnerLease slot_lock(slot->owner);
      slot->state = SlotState::open;
      return fail(result.error(), -1);
    }
  }
  {
    OwnerLease slot_lock(slot->owner);
    slot->state = SlotState::closed;
  }
  erase_directory(adapter, directory, slot);
  auto *token = reinterpret_cast<DirectoryToken *>(directory);
  if (token->magic == kDirectoryTokenMagic) {
    token->magic = 0;
  }
  delete token;
  return 0;
}
