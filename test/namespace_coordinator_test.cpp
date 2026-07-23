#include "orchfs/async/detail/namespace_coordinator.hpp"

#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <utility>
#include <vector>

namespace {

using orchfs::async::InodeNumber;
using orchfs::async::RangeMode;
using orchfs::async::Result;
using orchfs::async::Runtime;
using orchfs::async::Task;
using orchfs::async::detail::NamespaceCoordinator;

[[noreturn]] void fail(const char* message) {
  std::fprintf(stderr, "namespace coordinator test failure: %s\n", message);
  std::abort();
}

void require(bool condition, const char* message) {
  if (!condition) {
    fail(message);
  }
}

void update_maximum(std::atomic<unsigned>& maximum,
                    unsigned value) noexcept {
  unsigned observed = maximum.load(std::memory_order_relaxed);
  while (observed < value &&
         !maximum.compare_exchange_weak(
             observed, value, std::memory_order_relaxed,
             std::memory_order_relaxed)) {
  }
}

Task<Result<void>> hold_parent(
    NamespaceCoordinator& coordinator, InodeNumber parent,
    std::atomic<unsigned>& active, std::atomic<unsigned>& maximum,
    std::atomic<bool>& release,
    RangeMode mode = RangeMode::write) {
  auto acquired = co_await coordinator.acquire_parent(
      parent, mode);
  if (!acquired) {
    co_return Result<void>::failure(acquired.error());
  }
  auto guard = std::move(acquired).value();
  const unsigned now = active.fetch_add(1, std::memory_order_acq_rel) + 1;
  update_maximum(maximum, now);
  while (!release.load(std::memory_order_acquire)) {
    auto yielded = co_await Runtime::yield();
    if (!yielded) {
      active.fetch_sub(1, std::memory_order_acq_rel);
      auto ignored = co_await guard.release();
      (void)ignored;
      co_return Result<void>::failure(yielded.error());
    }
  }
  active.fetch_sub(1, std::memory_order_acq_rel);
  co_return co_await guard.release();
}

Task<Result<void>> hold_rename(
    NamespaceCoordinator& coordinator, InodeNumber first,
    InodeNumber second, std::atomic<unsigned>& acquired_count,
    std::atomic<bool>& release) {
  auto acquired = co_await coordinator.acquire_rename_parents(first, second);
  if (!acquired) {
    co_return Result<void>::failure(acquired.error());
  }
  auto guard = std::move(acquired).value();
  acquired_count.fetch_add(1, std::memory_order_release);
  while (!release.load(std::memory_order_acquire)) {
    ORCHFS_TRYV(co_await Runtime::yield());
  }
  co_return co_await guard.release();
}

Task<Result<void>> hold_directory_set(
    NamespaceCoordinator& coordinator,
    std::array<InodeNumber, 3> directories,
    std::atomic<unsigned>& acquired_count,
    std::atomic<bool>& release) {
  auto acquired = co_await coordinator.acquire_directory_set(directories);
  if (!acquired) {
    co_return Result<void>::failure(acquired.error());
  }
  auto guard = std::move(acquired).value();
  acquired_count.fetch_add(1, std::memory_order_release);
  while (!release.load(std::memory_order_acquire)) {
    ORCHFS_TRYV(co_await Runtime::yield());
  }
  co_return co_await guard.release();
}

Task<Result<void>> verify_inode_state(NamespaceCoordinator& coordinator,
                                      InodeNumber inode) {
  auto acquired = co_await coordinator.acquire_inode_state(inode);
  if (!acquired) {
    co_return Result<void>::failure(acquired.error());
  }
  auto guard = std::move(acquired).value();
  ORCHFS_TRYV(coordinator.retain_locked(inode));
  ORCHFS_TRYV(coordinator.retain_locked(inode));
  ORCHFS_TRY(orphaned, coordinator.mark_orphan_if_open_locked(inode));
  if (!orphaned) {
    co_return Result<void>::failure(
        std::make_error_code(std::errc::state_not_recoverable));
  }
  ORCHFS_TRY(first, coordinator.release_reference_locked(inode));
  if (first != NamespaceCoordinator::ReleaseDisposition::reference_remains) {
    co_return Result<void>::failure(
        std::make_error_code(std::errc::state_not_recoverable));
  }
  ORCHFS_TRY(last, coordinator.release_reference_locked(inode));
  if (last != NamespaceCoordinator::ReleaseDisposition::delete_orphan) {
    co_return Result<void>::failure(
        std::make_error_code(std::errc::state_not_recoverable));
  }
  coordinator.finish_orphan_release_locked(inode);
  co_return co_await guard.release();
}

template <typename Predicate>
void wait_until(Predicate predicate, const char* message) {
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5);
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      fail(message);
    }
    std::this_thread::yield();
  }
}

void join_all(std::vector<orchfs::async::JoinHandle<Result<void>>>& handles) {
  for (auto& handle : handles) {
    auto joined = std::move(handle).join();
    require(joined && joined.value(), "coordinator task failed");
  }
}

}  // namespace

int main() {
  orchfs::async::RuntimeOptions options;
  options.worker_count = 8;
  auto created = Runtime::create(std::move(options));
  require(static_cast<bool>(created), "Runtime create failed");
  auto runtime = std::move(created).value();
  NamespaceCoordinator coordinator(*runtime);

  std::atomic<unsigned> active{0};
  std::atomic<unsigned> maximum{0};
  std::atomic<bool> release{false};
  std::vector<orchfs::async::JoinHandle<Result<void>>> independent;
  for (InodeNumber parent = 100; parent < 108; ++parent) {
    auto submitted = runtime->submit(
        hold_parent(coordinator, parent, active, maximum, release));
    require(static_cast<bool>(submitted), "parent task submit failed");
    independent.push_back(std::move(submitted).value());
  }
  wait_until([&] { return active.load(std::memory_order_acquire) == 8; },
             "independent parents did not run concurrently");
  release.store(true, std::memory_order_release);
  join_all(independent);
  require(maximum.load(std::memory_order_acquire) == 8,
          "independent parent peak was not eight");

  active.store(0, std::memory_order_release);
  maximum.store(0, std::memory_order_release);
  release.store(false, std::memory_order_release);
  auto first = runtime->submit(
      hold_parent(coordinator, 200, active, maximum, release));
  require(static_cast<bool>(first), "first same-parent submit failed");
  wait_until([&] { return active.load(std::memory_order_acquire) == 1; },
             "first same-parent task did not acquire");
  auto second = runtime->submit(
      hold_parent(coordinator, 200, active, maximum, release));
  require(static_cast<bool>(second), "second same-parent submit failed");
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  require(active.load(std::memory_order_acquire) == 1,
          "same-parent writers overlapped");
  release.store(true, std::memory_order_release);
  std::vector<orchfs::async::JoinHandle<Result<void>>> same_parent;
  same_parent.push_back(std::move(first).value());
  same_parent.push_back(std::move(second).value());
  join_all(same_parent);

  active.store(0, std::memory_order_release);
  maximum.store(0, std::memory_order_release);
  release.store(false, std::memory_order_release);
  std::vector<orchfs::async::JoinHandle<Result<void>>> readers;
  for (unsigned index = 0; index < 2; ++index) {
    auto submitted = runtime->submit(hold_parent(
        coordinator, 210, active, maximum, release, RangeMode::read));
    require(static_cast<bool>(submitted), "same-parent reader submit failed");
    readers.push_back(std::move(submitted).value());
  }
  wait_until([&] { return active.load(std::memory_order_acquire) == 2; },
             "same-parent readers did not overlap");
  release.store(true, std::memory_order_release);
  join_all(readers);
  require(maximum.load(std::memory_order_acquire) == 2,
          "same-parent reader peak was not two");

  std::atomic<unsigned> rename_acquired{0};
  release.store(false, std::memory_order_release);
  auto disjoint_first = runtime->submit(
      hold_rename(coordinator, 301, 302, rename_acquired, release));
  auto disjoint_second = runtime->submit(
      hold_rename(coordinator, 303, 304, rename_acquired, release));
  require(disjoint_first && disjoint_second,
          "disjoint rename submit failed");
  wait_until(
      [&] { return rename_acquired.load(std::memory_order_acquire) == 2; },
      "disjoint rename locks did not overlap");
  release.store(true, std::memory_order_release);
  std::vector<orchfs::async::JoinHandle<Result<void>>> disjoint_renames;
  disjoint_renames.push_back(std::move(disjoint_first).value());
  disjoint_renames.push_back(std::move(disjoint_second).value());
  join_all(disjoint_renames);

  rename_acquired.store(0, std::memory_order_release);
  release.store(false, std::memory_order_release);
  auto forward = runtime->submit(
      hold_rename(coordinator, 311, 312, rename_acquired, release));
  auto reverse = runtime->submit(
      hold_rename(coordinator, 312, 311, rename_acquired, release));
  require(forward && reverse, "rename task submit failed");
  wait_until(
      [&] { return rename_acquired.load(std::memory_order_acquire) == 1; },
      "ordered rename lock did not acquire");
  release.store(true, std::memory_order_release);
  std::vector<orchfs::async::JoinHandle<Result<void>>> renames;
  renames.push_back(std::move(forward).value());
  renames.push_back(std::move(reverse).value());
  join_all(renames);
  require(rename_acquired.load(std::memory_order_acquire) == 2,
          "reverse rename lock did not complete");

  std::atomic<unsigned> set_acquired{0};
  release.store(false, std::memory_order_release);
  auto ordered = runtime->submit(hold_directory_set(
      coordinator, {321, 322, 323}, set_acquired, release));
  auto reverse_ordered = runtime->submit(hold_directory_set(
      coordinator, {323, 322, 321}, set_acquired, release));
  require(ordered && reverse_ordered, "directory-set submit failed");
  wait_until(
      [&] { return set_acquired.load(std::memory_order_acquire) == 1; },
      "directory set did not acquire");
  release.store(true, std::memory_order_release);
  std::vector<orchfs::async::JoinHandle<Result<void>>> directory_sets;
  directory_sets.push_back(std::move(ordered).value());
  directory_sets.push_back(std::move(reverse_ordered).value());
  join_all(directory_sets);
  require(set_acquired.load(std::memory_order_acquire) == 2,
          "reverse directory set did not complete");

  coordinator.cache_dentry(
      400, "child",
      NamespaceCoordinator::Dentry{
          .inode = 401, .offset = 128, .type = 1});
  const auto dentry = coordinator.find_dentry(400, "child");
  require(dentry && dentry->inode == 401 && dentry->offset == 128,
          "dentry cache lookup failed");
  coordinator.erase_dentry(400, "child");
  require(!coordinator.find_dentry(400, "child"),
          "dentry cache erase failed");

  coordinator.cache_dentry(
      410, "first",
      NamespaceCoordinator::Dentry{
          .inode = 411, .offset = 512, .type = 1});
  coordinator.cache_dentry(
      410, "second",
      NamespaceCoordinator::Dentry{
          .inode = 412, .offset = 768, .type = 1});
  coordinator.mark_directory_complete(410);
  require(coordinator.directory_complete(410),
          "directory completeness was not published");

  orchfs::async::FileStat snapshot{};
  snapshot.inode = 501;
  snapshot.size = 4096;
  coordinator.publish_snapshot(501, snapshot);
  const auto cached = coordinator.cached_snapshot(501);
  require(cached && cached.value().size == 4096,
          "snapshot cache lookup failed");
  coordinator.erase_snapshot(501);
  require(!coordinator.cached_snapshot(501),
          "snapshot cache erase failed");

  auto state = runtime->submit(verify_inode_state(coordinator, 601));
  require(static_cast<bool>(state), "inode state task submit failed");
  auto state_result = std::move(state).value().join();
  require(state_result && state_result.value(),
          "inode state lifecycle failed");

  runtime->request_stop();
  require(static_cast<bool>(runtime->join()), "Runtime join failed");
  return 0;
}
