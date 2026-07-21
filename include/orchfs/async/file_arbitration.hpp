#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>

#include "orchfs/async/range_arbiter.hpp"

namespace orchfs::async {

// Process-wide coroutine arbitration for filesystem metadata and file ranges.
// Readers use immutable snapshots; adding a previously unseen inode publishes a
// copy-on-write snapshot. Range waiters themselves are suspended by
// RangeArbiter and never block a Runtime worker in a pthread lock.
class FileArbitration final {
  using RangeMap =
      std::unordered_map<std::int64_t, std::shared_ptr<RangeArbiter>>;

 public:
  [[nodiscard]] std::shared_ptr<RangeArbiter> range_for(
      std::int64_t inode) {
    auto current = data_ranges_.load(std::memory_order_acquire);
    for (;;) {
      if (auto found = current->find(inode); found != current->end()) {
        return found->second;
      }

      auto range = std::make_shared<RangeArbiter>();
      auto updated = std::make_shared<RangeMap>(*current);
      updated->emplace(inode, range);
      std::shared_ptr<const RangeMap> published = std::move(updated);
      if (data_ranges_.compare_exchange_weak(
              current, std::move(published), std::memory_order_release,
              std::memory_order_acquire)) {
        return range;
      }
    }
  }

  [[nodiscard]] RangeArbiter& namespace_gate() noexcept {
    return namespace_gate_;
  }

 private:
  std::atomic<std::shared_ptr<const RangeMap>> data_ranges_{
      std::make_shared<const RangeMap>()};
  RangeArbiter namespace_gate_;
};

}  // namespace orchfs::async
