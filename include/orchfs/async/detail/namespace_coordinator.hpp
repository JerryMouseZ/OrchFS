#pragma once

#include "orchfs/async/filesystem.hpp"
#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/runtime.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>

namespace orchfs::async::detail {

// Owns namespace concurrency and its process-local indexes.  Callers keep
// filesystem semantics in KfsCoroutineCore while this module provides the
// narrow invariants: parent locks, ordered rename locking, inode-open state,
// and fixed-shard caches without whole-table publication copies.
class NamespaceCoordinator final {
 public:
  struct Dentry {
    InodeNumber inode{-1};
    std::uint64_t offset{};
    std::uint8_t type{};
  };

  enum class ReleaseDisposition : std::uint8_t {
    reference_remains,
    last_reference,
    delete_orphan,
  };

  class LockSet final {
   public:
    LockSet() = default;
    LockSet(const LockSet&) = delete;
    LockSet& operator=(const LockSet&) = delete;
    LockSet(LockSet&& other) noexcept;
    LockSet& operator=(LockSet&&) = delete;
    ~LockSet() = default;

    [[nodiscard]] Task<Result<void>> release();

   private:
    explicit LockSet(NamespaceCoordinator& coordinator) noexcept
        : coordinator_(&coordinator) {}

    void append(RangePermit permit) noexcept;
    std::size_t activate() noexcept;

    NamespaceCoordinator* coordinator_{};
    std::array<RangePermit, 4> permits_{};
    std::size_t count_{};
    bool active_{};

    friend class NamespaceCoordinator;
  };

  explicit NamespaceCoordinator(Runtime& runtime) noexcept
      : runtime_(&runtime) {}

  NamespaceCoordinator(const NamespaceCoordinator&) = delete;
  NamespaceCoordinator& operator=(const NamespaceCoordinator&) = delete;

  [[nodiscard]] Task<Result<LockSet>> acquire_parent(
      InodeNumber parent, RangeMode mode);
  [[nodiscard]] Task<Result<LockSet>> acquire_rename_parents(
      InodeNumber first_parent, InodeNumber second_parent);
  [[nodiscard]] Task<Result<LockSet>> acquire_directory_set(
      std::span<const InodeNumber> directories);
  [[nodiscard]] Task<Result<LockSet>> acquire_topology_change();
  [[nodiscard]] Task<Result<LockSet>> acquire_inode_state(
      InodeNumber inode);

  [[nodiscard]] Result<std::shared_ptr<RangeArbiter>> data_range(
      InodeNumber inode);

  [[nodiscard]] std::optional<Dentry> find_dentry(
      InodeNumber parent, std::string_view name);
  bool cache_dentry(InodeNumber parent, std::string_view name,
                    Dentry dentry) noexcept;
  [[nodiscard]] bool directory_complete(InodeNumber directory) noexcept;
  void erase_dentry(InodeNumber parent, std::string_view name) noexcept;
  void erase_directory(InodeNumber directory) noexcept;
  void mark_directory_complete(InodeNumber directory) noexcept;

  void publish_snapshot(InodeNumber inode, FileStat snapshot) noexcept;
  [[nodiscard]] Result<FileStat> cached_snapshot(
      InodeNumber inode) const noexcept;
  void erase_snapshot(InodeNumber inode) noexcept;

  // The matching inode-state LockSet must be held for these operations.
  [[nodiscard]] Result<void> retain_locked(InodeNumber inode);
  [[nodiscard]] Result<bool> mark_orphan_if_open_locked(
      InodeNumber inode) noexcept;
  void rollback_orphan_locked(InodeNumber inode) noexcept;
  [[nodiscard]] Result<ReleaseDisposition> release_reference_locked(
      InodeNumber inode) noexcept;
  void finish_orphan_release_locked(InodeNumber inode) noexcept;
  void erase_inode_state_locked(InodeNumber inode) noexcept;

 private:
  static constexpr std::size_t kShardCount = 256;
  static_assert((kShardCount & (kShardCount - 1U)) == 0);

  struct TransparentStringHash {
    using is_transparent = void;
    [[nodiscard]] std::size_t operator()(std::string_view value) const
        noexcept;
    [[nodiscard]] std::size_t operator()(const std::string& value) const
        noexcept {
      return (*this)(std::string_view(value));
    }
  };

  struct TransparentStringEqual {
    using is_transparent = void;
    [[nodiscard]] bool operator()(std::string_view left,
                                  std::string_view right) const noexcept {
      return left == right;
    }
  };

  using DentryMap = std::unordered_map<
      std::string, Dentry, TransparentStringHash, TransparentStringEqual>;

  struct DirectoryIndex {
    DentryMap names;
    bool complete{};
  };

  struct RangeShard {
    std::mutex mutex;
    std::unordered_map<InodeNumber, std::shared_ptr<RangeArbiter>> ranges;
  };

  struct DentryShard {
    std::mutex mutex;
    std::unordered_map<InodeNumber, DirectoryIndex> directories;
  };

  struct SnapshotShard {
    mutable std::mutex mutex;
    std::unordered_map<InodeNumber, FileStat> snapshots;
  };

  struct InodeState {
    std::size_t open_references{};
    bool orphaned{};
  };

  struct InodeStateShard {
    std::mutex mutex;
    std::unordered_map<InodeNumber, InodeState> states;
  };

  [[nodiscard]] static std::size_t shard_index(InodeNumber inode) noexcept;
  [[nodiscard]] Result<std::shared_ptr<RangeArbiter>> range_for(
      std::array<RangeShard, kShardCount>& shards, InodeNumber inode,
      bool bind_inode_owner);
  [[nodiscard]] Task<Result<void>> schedule_owner_if_unpinned(
      InodeNumber inode);
  [[nodiscard]] Task<Result<void>> acquire_into(
      LockSet& locks, RangeArbiter& range, RangeMode mode);
  [[nodiscard]] Task<Result<LockSet>> acquire_ordered(
      std::span<const InodeNumber> directories, RangeMode mode);
  void finish_namespace_trace(std::uint64_t started_ns,
                              std::size_t active, int error) noexcept;
  [[nodiscard]] std::size_t begin_active() noexcept;
  void end_active() noexcept;

  Runtime* runtime_{};
  std::array<RangeShard, kShardCount> data_ranges_{};
  std::array<RangeShard, kShardCount> inode_state_ranges_{};
  std::array<DentryShard, kShardCount> dentries_{};
  std::array<SnapshotShard, kShardCount> snapshots_{};
  std::array<InodeStateShard, kShardCount> inode_states_{};
  RangeArbiter topology_gate_;
#ifdef ORCHFS_REPRO_TRACE_ENABLED
  std::atomic<std::size_t> active_{0};
  std::atomic<std::size_t> peak_{0};
#endif
};

}  // namespace orchfs::async::detail
