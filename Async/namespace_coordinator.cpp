#include "orchfs/async/detail/namespace_coordinator.hpp"

#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/repro_trace.h"

#include <algorithm>
#include <cerrno>
#include <limits>
#include <new>
#include <utility>

namespace orchfs::async::detail {
namespace {

constexpr std::uint64_t kWholeInodeRange =
    std::numeric_limits<std::uint64_t>::max();

}  // namespace

NamespaceCoordinator::LockSet::LockSet(LockSet&& other) noexcept
    : coordinator_(std::exchange(other.coordinator_, nullptr)),
      permits_(std::move(other.permits_)),
      count_(std::exchange(other.count_, 0)),
      active_(std::exchange(other.active_, false)) {}

void NamespaceCoordinator::LockSet::append(RangePermit permit) noexcept {
  if (count_ >= permits_.size() || !permit.owns_lock()) {
    std::terminate();
  }
  permits_[count_++] = std::move(permit);
}

std::size_t NamespaceCoordinator::LockSet::activate() noexcept {
  if (coordinator_ == nullptr || active_) {
    std::terminate();
  }
  active_ = true;
  return coordinator_->begin_active();
}

Task<Result<void>> NamespaceCoordinator::LockSet::release() {
  Result<void> result = Result<void>::success();
  while (count_ != 0) {
    auto released = co_await permits_[--count_].release();
    if (!released && result) {
      result = Result<void>::failure(released.error());
    }
  }
  if (active_) {
    active_ = false;
    coordinator_->end_active();
  }
  co_return result;
}

std::size_t NamespaceCoordinator::TransparentStringHash::operator()(
    std::string_view value) const noexcept {
  std::uint64_t hash = 1469598103934665603ULL;
  for (const unsigned char byte : value) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return static_cast<std::size_t>(fmix64(hash));
}

std::size_t NamespaceCoordinator::shard_index(InodeNumber inode) noexcept {
  return static_cast<std::size_t>(
             fmix64(static_cast<std::uint64_t>(inode))) &
         (kShardCount - 1U);
}

Result<std::shared_ptr<RangeArbiter>> NamespaceCoordinator::range_for(
    std::array<RangeShard, kShardCount>& shards, InodeNumber inode,
    bool bind_inode_owner) {
  if (inode < 0) {
    return errno_failure<std::shared_ptr<RangeArbiter>>(EINVAL);
  }
  auto& shard = shards[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  if (const auto found = shard.ranges.find(inode);
      found != shard.ranges.end()) {
    return Result<std::shared_ptr<RangeArbiter>>::success(found->second);
  }
  try {
    auto range = bind_inode_owner
        ? std::make_shared<RangeArbiter>(
              *runtime_,
              runtime_->owner_for(static_cast<std::uint64_t>(inode)))
        : std::make_shared<RangeArbiter>();
    shard.ranges.emplace(inode, range);
    return Result<std::shared_ptr<RangeArbiter>>::success(std::move(range));
  } catch (const std::bad_alloc&) {
    return errno_failure<std::shared_ptr<RangeArbiter>>(ENOMEM);
  }
}

Result<std::shared_ptr<RangeArbiter>> NamespaceCoordinator::data_range(
    InodeNumber inode) {
  return range_for(data_ranges_, inode, true);
}

Task<Result<void>> NamespaceCoordinator::schedule_owner_if_unpinned(
    InodeNumber inode) {
  if (inode < 0) {
    co_return errno_failure<void>(EINVAL);
  }
  const auto target = current_resume_target();
  if (target.runtime != nullptr && target.runtime != runtime_) {
    co_return Result<void>::failure(Errc::wrong_runtime);
  }
  if (target.pin_depth != 0) {
    co_return Result<void>::success();
  }
  co_return co_await runtime_->schedule_on(
      runtime_->owner_for(static_cast<std::uint64_t>(inode)));
}

Task<Result<void>> NamespaceCoordinator::acquire_into(
    LockSet& locks, RangeArbiter& range, RangeMode mode) {
  auto acquired = co_await range.acquire(0, kWholeInodeRange, mode);
  if (!acquired) {
    co_return Result<void>::failure(acquired.error());
  }
  locks.append(std::move(acquired).value());
  co_return Result<void>::success();
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_parent(InodeNumber parent, RangeMode mode) {
  const std::array parents{parent};
  co_return co_await acquire_ordered(parents, mode);
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_rename_parents(
    InodeNumber first_parent, InodeNumber second_parent) {
  const std::array parents{first_parent, second_parent};
  co_return co_await acquire_ordered(parents, RangeMode::write);
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_directory_set(
    std::span<const InodeNumber> directories) {
  co_return co_await acquire_ordered(directories, RangeMode::write);
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_ordered(
    std::span<const InodeNumber> directories, RangeMode mode) {
  const std::uint64_t trace_started_ns = orchfs_repro_trace_begin();
  if (directories.empty() || directories.size() > 4) {
    finish_namespace_trace(trace_started_ns, 0, EINVAL);
    co_return errno_failure<LockSet>(EINVAL);
  }
  std::array<InodeNumber, 4> ordered{};
  std::copy(directories.begin(), directories.end(), ordered.begin());
  const auto ordered_end = ordered.begin() + directories.size();
  if (std::any_of(ordered.begin(), ordered_end,
                  [](InodeNumber inode) { return inode < 0; })) {
    finish_namespace_trace(trace_started_ns, 0, EINVAL);
    co_return errno_failure<LockSet>(EINVAL);
  }
  std::sort(ordered.begin(), ordered_end);
  const auto unique_end = std::unique(ordered.begin(), ordered_end);

  auto scheduled = co_await schedule_owner_if_unpinned(ordered.front());
  if (!scheduled) {
    finish_namespace_trace(trace_started_ns, 0, scheduled.error().value());
    co_return Result<LockSet>::failure(scheduled.error());
  }
  LockSet locks(*this);
  for (auto iterator = ordered.begin(); iterator != unique_end; ++iterator) {
    auto range = data_range(*iterator);
    Result<void> acquired = range
        ? co_await acquire_into(locks, *range.value(), mode)
        : Result<void>::failure(range.error());
    if (!acquired) {
      const auto error = acquired.error();
      auto released = co_await locks.release();
      (void)released;
      finish_namespace_trace(trace_started_ns, 0, error.value());
      co_return Result<LockSet>::failure(error);
    }
  }
  const std::size_t active = locks.activate();
  finish_namespace_trace(trace_started_ns, active, 0);
  co_return Result<LockSet>::success(std::move(locks));
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_topology_change() {
  auto scheduled = co_await schedule_owner_if_unpinned(0);
  if (!scheduled) {
    co_return Result<LockSet>::failure(scheduled.error());
  }
  LockSet locks(*this);
  auto acquired = co_await acquire_into(
      locks, topology_gate_, RangeMode::write);
  if (!acquired) {
    co_return Result<LockSet>::failure(acquired.error());
  }
  co_return Result<LockSet>::success(std::move(locks));
}

Task<Result<NamespaceCoordinator::LockSet>>
NamespaceCoordinator::acquire_inode_state(InodeNumber inode) {
  auto scheduled = co_await schedule_owner_if_unpinned(inode);
  if (!scheduled) {
    co_return Result<LockSet>::failure(scheduled.error());
  }
  auto range = range_for(inode_state_ranges_, inode, false);
  if (!range) {
    co_return Result<LockSet>::failure(range.error());
  }
  LockSet locks(*this);
  auto acquired = co_await acquire_into(
      locks, *range.value(), RangeMode::write);
  if (!acquired) {
    co_return Result<LockSet>::failure(acquired.error());
  }
  co_return Result<LockSet>::success(std::move(locks));
}

std::optional<NamespaceCoordinator::Dentry>
NamespaceCoordinator::find_dentry(InodeNumber parent, std::string_view name) {
  if (parent < 0) {
    return std::nullopt;
  }
  auto& shard = dentries_[shard_index(parent)];
  std::lock_guard lock(shard.mutex);
  const auto directory = shard.directories.find(parent);
  if (directory == shard.directories.end()) {
    return std::nullopt;
  }
  const auto found = directory->second.names.find(name);
  return found == directory->second.names.end()
             ? std::nullopt
             : std::optional<Dentry>(found->second);
}

bool NamespaceCoordinator::cache_dentry(
    InodeNumber parent, std::string_view name, Dentry dentry) noexcept {
  if (parent < 0) {
    return false;
  }
  auto& shard = dentries_[shard_index(parent)];
  std::lock_guard lock(shard.mutex);
  try {
    auto& directory = shard.directories[parent];
    if (const auto found = directory.names.find(name);
        found != directory.names.end()) {
      found->second = dentry;
    } else {
      directory.names.emplace(std::string(name), dentry);
    }
    return true;
  } catch (const std::bad_alloc&) {
    // The cache is an optimization; on allocation pressure the durable
    // directory remains authoritative.  Preserve positive entries populated
    // by concurrent readers, but force future misses back to a durable scan.
    if (const auto found = shard.directories.find(parent);
        found != shard.directories.end()) {
      found->second.complete = false;
    }
    return false;
  }
}

bool NamespaceCoordinator::directory_complete(InodeNumber directory) noexcept {
  if (directory < 0) {
    return false;
  }
  auto& shard = dentries_[shard_index(directory)];
  std::lock_guard lock(shard.mutex);
  const auto found = shard.directories.find(directory);
  return found != shard.directories.end() && found->second.complete;
}

void NamespaceCoordinator::erase_dentry(
    InodeNumber parent, std::string_view name) noexcept {
  if (parent < 0) {
    return;
  }
  auto& shard = dentries_[shard_index(parent)];
  std::lock_guard lock(shard.mutex);
  const auto directory = shard.directories.find(parent);
  if (directory == shard.directories.end()) {
    return;
  }
  if (const auto found = directory->second.names.find(name);
      found != directory->second.names.end()) {
    directory->second.names.erase(found);
  }
  if (directory->second.names.empty() && !directory->second.complete) {
    shard.directories.erase(directory);
  }
}

void NamespaceCoordinator::erase_directory(InodeNumber directory) noexcept {
  if (directory < 0) {
    return;
  }
  auto& shard = dentries_[shard_index(directory)];
  std::lock_guard lock(shard.mutex);
  shard.directories.erase(directory);
}

void NamespaceCoordinator::mark_directory_complete(
    InodeNumber directory) noexcept {
  if (directory < 0) {
    return;
  }
  auto& shard = dentries_[shard_index(directory)];
  std::lock_guard lock(shard.mutex);
  try {
    auto& index = shard.directories[directory];
    index.complete = true;
  } catch (const std::bad_alloc&) {
    shard.directories.erase(directory);
  }
}

void NamespaceCoordinator::publish_snapshot(
    InodeNumber inode, FileStat snapshot) noexcept {
  if (inode < 0) {
    return;
  }
  try {
    auto& shard = snapshots_[shard_index(inode)];
    std::lock_guard lock(shard.mutex);
    shard.snapshots.insert_or_assign(inode, std::move(snapshot));
  } catch (const std::bad_alloc&) {
    // A missed cache publication is safe; the next stat reloads the inode.
  }
}

Result<FileStat> NamespaceCoordinator::cached_snapshot(
    InodeNumber inode) const noexcept {
  if (inode < 0) {
    return errno_failure<FileStat>(EINVAL);
  }
  const auto& shard = snapshots_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  const auto found = shard.snapshots.find(inode);
  return found == shard.snapshots.end()
             ? errno_failure<FileStat>(ENOENT)
             : Result<FileStat>::success(found->second);
}

void NamespaceCoordinator::erase_snapshot(InodeNumber inode) noexcept {
  if (inode < 0) {
    return;
  }
  auto& shard = snapshots_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  shard.snapshots.erase(inode);
}

Result<void> NamespaceCoordinator::retain_locked(InodeNumber inode) {
  if (inode < 0) {
    return errno_failure<void>(EINVAL);
  }
  auto& shard = inode_states_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  try {
    auto& state = shard.states[inode];
    if (state.open_references ==
        std::numeric_limits<std::size_t>::max()) {
      return errno_failure<void>(EOVERFLOW);
    }
    ++state.open_references;
    return Result<void>::success();
  } catch (const std::bad_alloc&) {
    return errno_failure<void>(ENOMEM);
  }
}

Result<bool> NamespaceCoordinator::mark_orphan_if_open_locked(
    InodeNumber inode) noexcept {
  if (inode < 0) {
    return errno_failure<bool>(EINVAL);
  }
  auto& shard = inode_states_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  const auto found = shard.states.find(inode);
  if (found == shard.states.end() || found->second.open_references == 0) {
    return Result<bool>::success(false);
  }
  found->second.orphaned = true;
  return Result<bool>::success(true);
}

void NamespaceCoordinator::rollback_orphan_locked(InodeNumber inode) noexcept {
  if (inode < 0) {
    return;
  }
  auto& shard = inode_states_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  if (const auto found = shard.states.find(inode);
      found != shard.states.end()) {
    found->second.orphaned = false;
  }
}

Result<NamespaceCoordinator::ReleaseDisposition>
NamespaceCoordinator::release_reference_locked(InodeNumber inode) noexcept {
  if (inode < 0) {
    return errno_failure<ReleaseDisposition>(EINVAL);
  }
  auto& shard = inode_states_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  const auto found = shard.states.find(inode);
  if (found == shard.states.end() || found->second.open_references == 0) {
    return errno_failure<ReleaseDisposition>(EBADF);
  }
  if (found->second.open_references > 1) {
    --found->second.open_references;
    return Result<ReleaseDisposition>::success(
        ReleaseDisposition::reference_remains);
  }
  if (!found->second.orphaned) {
    shard.states.erase(found);
    return Result<ReleaseDisposition>::success(
        ReleaseDisposition::last_reference);
  }
  return Result<ReleaseDisposition>::success(
      ReleaseDisposition::delete_orphan);
}

void NamespaceCoordinator::finish_orphan_release_locked(
    InodeNumber inode) noexcept {
  erase_inode_state_locked(inode);
}

void NamespaceCoordinator::erase_inode_state_locked(
    InodeNumber inode) noexcept {
  if (inode < 0) {
    return;
  }
  auto& shard = inode_states_[shard_index(inode)];
  std::lock_guard lock(shard.mutex);
  shard.states.erase(inode);
}

std::size_t NamespaceCoordinator::begin_active() noexcept {
#ifdef ORCHFS_REPRO_TRACE_ENABLED
  const std::size_t active =
      active_.fetch_add(1, std::memory_order_acq_rel) + 1;
  auto observed = peak_.load(std::memory_order_relaxed);
  while (observed < active &&
         !peak_.compare_exchange_weak(
             observed, active, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
  return active;
#else
  return 0;
#endif
}

void NamespaceCoordinator::end_active() noexcept {
#ifdef ORCHFS_REPRO_TRACE_ENABLED
  const std::size_t previous = active_.fetch_sub(1, std::memory_order_acq_rel);
  if (previous == 0) {
    std::terminate();
  }
#endif
}

void NamespaceCoordinator::finish_namespace_trace(
    std::uint64_t started_ns, std::size_t active, int error) noexcept {
#ifdef ORCHFS_REPRO_TRACE_ENABLED
  const std::size_t peak = peak_.load(std::memory_order_relaxed);
#else
  const std::size_t peak = 0;
#endif
  orchfs_repro_trace_end(
      ORCHFS_TRACE_NAMESPACE_WAIT, 0, started_ns, active,
      static_cast<std::uint32_t>(std::min<std::size_t>(
          peak, std::numeric_limits<std::uint32_t>::max())),
      error);
}

}  // namespace orchfs::async::detail
