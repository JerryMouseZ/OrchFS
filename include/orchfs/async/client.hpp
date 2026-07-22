#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

#include "orchfs/async/filesystem.hpp"
#include "orchfs/async/result.hpp"
#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/rpc_protocol.hpp"
#include "orchfs/async/task.hpp"

namespace orchfs::async {

class Runtime;
class Session;
class File;
class Directory;

struct ClientOptions {
  std::string endpoint{"/tmp/orchfs-kfs.sock"};
  // Zero preserves the native default of one lane per Runtime worker.  A
  // blocking adapter may use more transport lanes than polling workers so a
  // single hot client worker can still feed every server lane.
  std::size_t lane_count{0};
  std::size_t ring_capacity{64};
  std::size_t data_slot_size{1024U * 1024U};
};

class Client {
 public:
  Client() noexcept = default;
  Client(Client&&) noexcept;
  Client& operator=(Client&&) noexcept;
  Client(const Client&) = delete;
  Client& operator=(const Client&) = delete;
  ~Client();

  static Task<Result<Client>> connect(Runtime& runtime,
                                      ClientOptions options = {});

  Task<Result<File>> open(std::string path, int flags,
                          std::uint32_t mode = 0644);
  Task<Result<File>> open_at(const Directory& directory,
                             std::string path, int flags,
                             std::uint32_t mode = 0644);
  Task<Result<Directory>> open_directory(std::string path);
  // Duplicate an already-open directory File into an independent directory
  // cursor. Resolution is anchored by the remote file handle, so pathname
  // rename/recreate races cannot change the inode selected for openat().
  Task<Result<Directory>> open_directory(const File& file);
  Task<Result<FileStat>> stat(std::string path);
  Task<Result<void>> make_directory(std::string path,
                                    std::uint32_t mode = 0755);
  Task<Result<void>> remove_directory(std::string path);
  Task<Result<void>> unlink(std::string path);
  Task<Result<void>> rename(std::string old_path,
                            std::string new_path);
  Task<Result<void>> truncate(std::string path, std::uint64_t size);
  Task<Result<void>> shutdown();

  [[nodiscard]] bool valid() const noexcept;

 private:
  explicit Client(std::shared_ptr<Session> session) noexcept;
  std::shared_ptr<Session> session_;

  friend class File;
  friend class Directory;
};

class File {
 public:
  File() noexcept = default;
  File(File&&) noexcept;
  File& operator=(File&&) noexcept;
  File(const File&) = delete;
  File& operator=(const File&) = delete;
  ~File();

  Task<Result<std::size_t>> read(std::span<std::byte> buffer);
  Task<Result<std::size_t>> write(std::span<const std::byte> buffer);
  Task<Result<std::size_t>> read_at(std::uint64_t offset,
                                    std::span<std::byte> buffer);
  Task<Result<std::size_t>> write_at(std::uint64_t offset,
                                     std::span<const std::byte> buffer);
  // Blocking compatibility seam for external syscall threads. These methods
  // submit directly to the Client session, so the caller holds a leased CQ
  // slot and performs the read copy itself instead of resuming a root
  // coroutine on the client worker. Calling them from a Runtime worker fails.
  Result<std::size_t> read_at_blocking(std::uint64_t offset,
                                       std::span<std::byte> buffer);
  Result<std::size_t> write_at_blocking(
      std::uint64_t offset, std::span<const std::byte> buffer);
  Task<Result<std::uint64_t>> seek(std::int64_t offset, int whence);
  Task<Result<FileStat>> stat();
  Task<Result<FileSystemStat>> statfs();
  Task<Result<void>> truncate(std::uint64_t size);
  Task<Result<void>> sync();
  Task<Result<int>> get_flags();
  Task<Result<void>> set_flags(int flags);
  Task<Result<void>> close();

  [[nodiscard]] bool valid() const noexcept;

 private:
  Task<Result<std::size_t>> read_unlocked(std::span<std::byte> buffer);
  Task<Result<std::size_t>> write_unlocked(
      std::span<const std::byte> buffer);
  Task<Result<std::uint64_t>> seek_unlocked(std::int64_t offset, int whence);

  File(std::shared_ptr<Session> session, RemoteHandle handle) noexcept;
  std::shared_ptr<Session> session_;
  RemoteHandle handle_{kInvalidRemoteHandle};
  RangeArbiter offset_gate_;

  friend class Client;
};

class Directory {
 public:
  Directory() noexcept = default;
  Directory(Directory&&) noexcept;
  Directory& operator=(Directory&&) noexcept;
  Directory(const Directory&) = delete;
  Directory& operator=(const Directory&) = delete;
  ~Directory();

  Task<Result<std::size_t>> next_batch(std::span<DirEntry> entries);
  Task<Result<void>> close();

  [[nodiscard]] bool valid() const noexcept;

 private:
  Directory(std::shared_ptr<Session> session, RemoteHandle handle) noexcept;
  std::shared_ptr<Session> session_;
  RemoteHandle handle_{kInvalidRemoteHandle};

  friend class Client;
};

}  // namespace orchfs::async
