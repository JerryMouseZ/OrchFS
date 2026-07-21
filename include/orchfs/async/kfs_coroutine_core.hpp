#pragma once

#include "orchfs/async/filesystem.hpp"
#include "orchfs/async/result.hpp"

#include <cstddef>
#include <memory>

namespace orchfs::async {

class Runtime;

// Production AsyncFilesystem adapter for the on-media OrchFS layout.  The
// module converts logical file operations into explicit asynchronous device
// phases; no blocking fd or device wrapper is part of its interface.
class KfsCoroutineCore final : public AsyncFilesystem {
 public:
  [[nodiscard]] static Result<std::shared_ptr<KfsCoroutineCore>> create(
      Runtime& runtime);

  ~KfsCoroutineCore() override;

  KfsCoroutineCore(const KfsCoroutineCore&) = delete;
  KfsCoroutineCore& operator=(const KfsCoroutineCore&) = delete;

  [[nodiscard]] Task<Result<OpenedNode>> open(
      std::string path, int flags, std::uint32_t mode) override;
  [[nodiscard]] Task<Result<OpenedNode>> open_at(
      InodeNumber directory, std::string path, int flags,
      std::uint32_t mode) override;
  [[nodiscard]] Task<Result<void>> close(OpenedNode node) override;

  [[nodiscard]] Task<Result<std::size_t>> read(
      InodeNumber inode, std::uint64_t offset,
      std::span<std::byte> destination) override;
  [[nodiscard]] Task<Result<WriteResult>> write(
      InodeNumber inode, std::uint64_t offset,
      std::span<const std::byte> source, bool append) override;

  [[nodiscard]] Task<Result<FileStat>> stat(std::string path) override;
  [[nodiscard]] Task<Result<FileStat>> stat(InodeNumber inode) override;
  [[nodiscard]] Task<Result<FileSystemStat>> statfs(
      InodeNumber inode) override;
  [[nodiscard]] Task<Result<std::uint64_t>> seek(
      InodeNumber inode, std::uint64_t current_offset,
      std::int64_t offset, int whence) override;
  [[nodiscard]] Task<Result<void>> truncate(
      std::string path, std::uint64_t size) override;
  [[nodiscard]] Task<Result<void>> truncate(
      InodeNumber inode, std::uint64_t size) override;
  [[nodiscard]] Task<Result<void>> sync(InodeNumber inode) override;

  [[nodiscard]] Task<Result<void>> make_directory(
      std::string path, std::uint32_t mode) override;
  [[nodiscard]] Task<Result<void>> remove_directory(
      std::string path) override;
  [[nodiscard]] Task<Result<void>> unlink(std::string path) override;
  [[nodiscard]] Task<Result<void>> rename(
      std::string old_path, std::string new_path) override;

  [[nodiscard]] Task<Result<OpenedNode>> open_directory(
      std::string path) override;
  [[nodiscard]] Task<Result<OpenedNode>> open_directory(
      InodeNumber inode) override;
  [[nodiscard]] Task<Result<DirectoryReadResult>> read_directory(
      InodeNumber inode, std::uint64_t cursor,
      std::span<DirEntry> entries) override;
  [[nodiscard]] Task<Result<void>> close_directory(
      OpenedNode node) override;

  // Drain up to max_operations from the Runtime-owned migration inbox using
  // the same namespace gate, inode range map, allocator, and device phases as
  // foreground I/O. The bool result reports whether more candidates remain.
  [[nodiscard]] Task<Result<bool>> migrate(std::size_t max_operations);

 private:
  class Impl;

  explicit KfsCoroutineCore(std::unique_ptr<Impl> impl) noexcept;
  std::unique_ptr<Impl> impl_;
};

}  // namespace orchfs::async
