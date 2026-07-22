#include "orchfs/async/kfs_coroutine_core.hpp"

#include "orchfs/async/block_device.hpp"
#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/async/detail/range_lock.hpp"
#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/runtime.hpp"

#include <boost/container/small_vector.hpp>

extern "C" {
#include "../LibFS/kfs_core_api.h"
#include "../LibFS/lib_log.h"
#include "../LibFS/migrate.h"
}

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <linux/magic.h>
#include <memory>
#include <new>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace {

constexpr std::uint64_t kPageSize = 4096;
constexpr std::uint64_t kBlockSize = 32U * 1024U;
constexpr std::size_t kBufferMetadataSize = 128;
constexpr std::size_t kBufferMetadataEntries =
    kBufferMetadataSize / sizeof(std::int16_t);
constexpr std::int16_t kMaximumBufferSegments = 12;
constexpr std::uint64_t kMaximumFileSize =
    std::uint64_t{1} << (42U + 15U);
constexpr std::uint64_t kWholeFile =
    std::numeric_limits<std::uint64_t>::max();
constexpr std::size_t kYieldBytes = 1024U * 1024U;
constexpr std::size_t kDirectoryWindowEntries = 64;
constexpr int kSsdBlockExtent = 6;

static_assert(ORCHFS_CORE_PAGE_COUNT == kBlockSize / kPageSize);
static_assert(sizeof(orchfs_core_dirent) == ORCHFS_CORE_DIRENT_SIZE);

using detail::errno_failure;

NodeType node_type(int type) noexcept {
  switch (type) {
    case ORCHFS_CORE_DIRECTORY:
      return NodeType::directory;
    case ORCHFS_CORE_REGULAR:
      return NodeType::regular;
    default:
      return NodeType::unknown;
  }
}

OpenedNode opened_node(const orchfs_core_inode& inode) noexcept {
  return OpenedNode{.inode = inode.inode, .type = node_type(inode.type)};
}

FileStat file_stat(const orchfs_core_inode& inode) noexcept {
  return FileStat{
      .device = 0,
      .inode = static_cast<std::uint64_t>(inode.inode),
      .mode = static_cast<std::uint64_t>(
          static_cast<std::uint32_t>(inode.mode)),
      .link_count = static_cast<std::uint64_t>(inode.link_count),
      .uid = static_cast<std::uint64_t>(static_cast<std::uint32_t>(inode.uid)),
      .gid = static_cast<std::uint64_t>(static_cast<std::uint32_t>(inode.gid)),
      .rdev = 0,
      .size = inode.size,
      .block_size = static_cast<std::int64_t>(kPageSize),
      .blocks = inode.size <= 0 ? 0 : (inode.size + 511) / 512,
      .atime_seconds = inode.atime_seconds,
      .atime_nanoseconds = inode.atime_nanoseconds,
      .mtime_seconds = inode.mtime_seconds,
      .mtime_nanoseconds = inode.mtime_nanoseconds,
      .ctime_seconds = inode.ctime_seconds,
      .ctime_nanoseconds = inode.ctime_nanoseconds,
  };
}

std::uint64_t range_length(std::uint64_t offset, std::size_t length) noexcept {
  if (length == 0) {
    return 1;
  }
  return length > kWholeFile - offset ? kWholeFile : length;
}

struct ReadScratch {
  std::vector<std::byte> block;
  std::byte* destination{};
  std::size_t block_offset{};
  std::size_t length{};
  orchfs_core_block mapping{};
  bool strata{};
};

struct PmemWrite {
  std::int64_t offset{};
  const std::byte* source{};
  std::size_t length{};
};

struct MetadataWrite {
  std::int64_t offset{};
  std::array<std::int16_t, kBufferMetadataEntries> value{};
};

struct MergePage {
  std::vector<std::byte> page;
  std::int64_t ssd_offset{};
  std::int64_t nvm_offset{};
  std::array<std::int16_t, kBufferMetadataEntries> metadata{};
};

struct CachedExtent {
  std::uint64_t first_block{};
  std::uint64_t block_count{};
  orchfs_core_block mapping{};
};

struct ExtentSnapshot {
  std::uint64_t generation{};
  std::uint64_t file_size{};
  std::vector<CachedExtent> extents;
};

struct ExtentCacheEntry {
  std::atomic<InodeNumber> inode{-1};
  std::atomic<std::uint64_t> next_generation{1};
  std::atomic<std::shared_ptr<const ExtentSnapshot>> snapshot{};
};

bool mergeable_ssd_extent(const CachedExtent& previous,
                          std::uint64_t logical_block,
                          const orchfs_core_block& mapping) noexcept {
  if (previous.mapping.type != ORCHFS_CORE_SSD_BLOCK ||
      mapping.type != ORCHFS_CORE_SSD_BLOCK ||
      previous.first_block + previous.block_count != logical_block ||
      previous.mapping.ssd_device_offset < 0 ||
      mapping.ssd_device_offset < 0) {
    return false;
  }
  const auto count = static_cast<std::int64_t>(previous.block_count);
  return count <=
             (std::numeric_limits<std::int64_t>::max() -
              previous.mapping.ssd_device_offset) /
                 static_cast<std::int64_t>(kBlockSize) &&
         previous.mapping.ssd_device_offset +
                 count * static_cast<std::int64_t>(kBlockSize) ==
             mapping.ssd_device_offset;
}

void append_extent(std::vector<CachedExtent>& extents,
                   std::uint64_t logical_block,
                   const orchfs_core_block& mapping) {
  if (!extents.empty() &&
      mergeable_ssd_extent(extents.back(), logical_block, mapping)) {
    ++extents.back().block_count;
    return;
  }
  extents.push_back(CachedExtent{
      .first_block = logical_block,
      .block_count = 1,
      .mapping = mapping,
  });
}

void append_cached_extent(std::vector<CachedExtent>& extents,
                          CachedExtent extent) {
  if (extent.block_count == 0) {
    return;
  }
  if (!extents.empty() && extent.mapping.type == ORCHFS_CORE_SSD_BLOCK &&
      mergeable_ssd_extent(extents.back(), extent.first_block,
                           extent.mapping)) {
    extents.back().block_count += extent.block_count;
    return;
  }
  extents.push_back(std::move(extent));
}

bool mapping_from_snapshot(const ExtentSnapshot& snapshot,
                           std::uint64_t logical_block,
                           orchfs_core_block& mapping) noexcept {
  const auto found = std::upper_bound(
      snapshot.extents.begin(), snapshot.extents.end(), logical_block,
      [](std::uint64_t block, const CachedExtent& extent) {
        return block < extent.first_block;
      });
  if (found == snapshot.extents.begin()) {
    return false;
  }
  const auto& extent = *std::prev(found);
  if (logical_block < extent.first_block ||
      logical_block - extent.first_block >= extent.block_count) {
    return false;
  }
  mapping = extent.mapping;
  if (mapping.type == ORCHFS_CORE_SSD_BLOCK) {
    const auto delta = logical_block - extent.first_block;
    if (delta > static_cast<std::uint64_t>(
                    std::numeric_limits<std::int64_t>::max() /
                    static_cast<std::int64_t>(kBlockSize))) {
      return false;
    }
    mapping.ssd_device_offset +=
        static_cast<std::int64_t>(delta * kBlockSize);
    mapping.file_block += static_cast<std::int64_t>(delta);
  }
  return true;
}

int read_metadata(
    std::int64_t offset,
    std::array<std::int16_t, kBufferMetadataEntries>& metadata) noexcept {
  if (offset < 0) {
    return EIO;
  }
  const int error = orchfs_core_read_pmem(offset, metadata.data(),
                                          kBufferMetadataSize);
  if (error != 0) {
    return error;
  }
  return metadata[0] < 0 || metadata[0] > kMaximumBufferSegments ? EIO : 0;
}

int overlay_page(
    std::span<std::byte> destination, std::int64_t nvm_offset,
    const std::array<std::int16_t, kBufferMetadataEntries>& metadata) {
  if (nvm_offset < 0 || destination.size() != kPageSize) {
    return EIO;
  }
  std::array<std::byte, kPageSize> cached{};
  int error = orchfs_core_read_pmem(nvm_offset, cached.data(), cached.size());
  if (error != 0) {
    return error;
  }
  for (std::int16_t segment = 0; segment < metadata[0]; ++segment) {
    const auto start = metadata[1 + segment * 2];
    const auto length = metadata[2 + segment * 2];
    if (start < 0 || length < 0 ||
        static_cast<std::size_t>(start) > destination.size() ||
        static_cast<std::size_t>(length) >
            destination.size() - static_cast<std::size_t>(start)) {
      return EIO;
    }
    std::memcpy(destination.data() + start, cached.data() + start,
                static_cast<std::size_t>(length));
  }
  return 0;
}

class LogTransaction final {
 public:
  static Result<std::unique_ptr<LogTransaction>> create() {
    orchfs_log_transaction* transaction = nullptr;
    const int error = orchfs_log_transaction_create(&transaction);
    if (error != 0) {
      return errno_failure<std::unique_ptr<LogTransaction>>(error);
    }
    try {
      return Result<std::unique_ptr<LogTransaction>>::success(
          std::unique_ptr<LogTransaction>(new LogTransaction(transaction)));
    } catch (const std::bad_alloc&) {
      orchfs_log_transaction_abort(transaction);
      return errno_failure<std::unique_ptr<LogTransaction>>(ENOMEM);
    }
  }

  ~LogTransaction() {
    if (transaction_ != nullptr) {
      orchfs_log_transaction_abort(transaction_);
    }
  }

  LogTransaction(const LogTransaction&) = delete;
  LogTransaction& operator=(const LogTransaction&) = delete;

  template <typename Function>
  decltype(auto) invoke(Function&& function) {
    orchfs_log_transaction_bind(transaction_);
    if constexpr (std::is_void_v<std::invoke_result_t<Function>>) {
      std::forward<Function>(function)();
      orchfs_log_transaction_unbind(transaction_);
    } else {
      auto result = std::forward<Function>(function)();
      orchfs_log_transaction_unbind(transaction_);
      return result;
    }
  }

  int add_pmem(std::int64_t offset, const void* data,
               std::size_t length) noexcept {
    if (length > static_cast<std::size_t>(
                     std::numeric_limits<std::int32_t>::max())) {
      return EOVERFLOW;
    }
    return orchfs_log_transaction_add_pmem(
        transaction_, offset, data, static_cast<std::int32_t>(length));
  }

  orchfs_log_transaction* release() noexcept {
    return std::exchange(transaction_, nullptr);
  }

  orchfs_log_transaction* get() const noexcept { return transaction_; }

 private:
  explicit LogTransaction(orchfs_log_transaction* transaction) noexcept
      : transaction_(transaction) {}

  orchfs_log_transaction* transaction_{};
};

class JournalService final {
 public:
  explicit JournalService(Runtime& runtime) : runtime_(&runtime) {
    auto registered = runtime.register_poller(0, &JournalService::poll, this);
    if (!registered) {
      throw std::system_error(registered.error());
    }
    registration_ = std::move(registered).value();
  }

  ~JournalService() {
    registration_.reset();
    if (inbox_.load(std::memory_order_acquire) != nullptr || local_ != nullptr) {
      std::terminate();
    }
  }

  JournalService(const JournalService&) = delete;
  JournalService& operator=(const JournalService&) = delete;

  class CommitAwaiter final {
   public:
    enum class State : std::uint8_t { submitting, suspended, completed };

    CommitAwaiter(JournalService& service,
                  orchfs_log_transaction* transaction) noexcept
        : service_(&service), transaction_(transaction) {
      if (transaction_ != nullptr &&
          orchfs_log_transaction_is_empty(transaction_) != 0) {
        const int error = orchfs_log_transaction_commit(
            std::exchange(transaction_, nullptr));
        if (error != 0) {
          error_ = std::error_code(error, std::generic_category());
        }
      }
    }

    bool await_ready() const noexcept { return transaction_ == nullptr; }

    bool await_suspend(std::coroutine_handle<> continuation) noexcept {
      if (Runtime::current() != service_->runtime_ ||
          Runtime::current_worker() == detail::no_worker) {
        error_ = make_error_code(Errc::not_in_runtime);
        orchfs_log_transaction_abort(
            std::exchange(transaction_, nullptr));
        return false;
      }
      continuation_ = continuation;
      worker_ = Runtime::current_worker();
      auto* head = service_->inbox_.load(std::memory_order_relaxed);
      do {
        next_ = head;
      } while (!service_->inbox_.compare_exchange_weak(
          head, this, std::memory_order_release, std::memory_order_relaxed));
      if (!service_->runtime_->notify(0)) {
        std::terminate();
      }
      const auto previous = state_.exchange(
          State::suspended, std::memory_order_acq_rel);
      if (previous == State::completed) {
        continuation_ = {};
        return false;
      }
      if (previous != State::submitting) {
        std::terminate();
      }
      return true;
    }

    Result<void> await_resume() const noexcept {
      return error_ ? Result<void>::failure(error_)
                    : Result<void>::success();
    }

   private:
    orchfs_log_transaction* take_transaction() noexcept {
      return std::exchange(transaction_, nullptr);
    }

    void complete_on_owner(int error) noexcept {
      if (error != 0) {
        error_ = std::error_code(error, std::generic_category());
      }
      const auto previous = state_.exchange(
          State::completed, std::memory_order_acq_rel);
      if (previous == State::submitting) {
        return;
      }
      if (previous != State::suspended) {
        std::terminate();
      }
      const auto continuation =
          std::exchange(continuation_, std::coroutine_handle<>{});
      if (!continuation ||
          !service_->runtime_->schedule(continuation, worker_)) {
        std::terminate();
      }
    }

    JournalService* service_{};
    orchfs_log_transaction* transaction_{};
    CommitAwaiter* next_{};
    std::size_t worker_{detail::no_worker};
    std::coroutine_handle<> continuation_{};
    std::error_code error_;
    std::atomic<State> state_{State::submitting};

    friend class JournalService;
  };

  CommitAwaiter commit(orchfs_log_transaction* transaction) noexcept {
    return CommitAwaiter(*this, transaction);
  }

 private:
  static Runtime::PollState poll(void* context) noexcept {
    return static_cast<JournalService*>(context)->poll_once();
  }

  Runtime::PollState poll_once() noexcept {
    if (local_ == nullptr) {
      auto* stack = inbox_.exchange(nullptr, std::memory_order_acquire);
      while (stack != nullptr) {
        auto* next = stack->next_;
        stack->next_ = local_;
        local_ = stack;
        stack = next;
      }
    }
    if (local_ == nullptr) {
      return Runtime::PollState::idle;
    }

    constexpr std::size_t kMaximumCommitBatch = 64;
    std::array<CommitAwaiter*, kMaximumCommitBatch> requests{};
    std::array<orchfs_log_transaction*, kMaximumCommitBatch> transactions{};
    std::array<int, kMaximumCommitBatch> errors{};
    std::size_t count = 0;
    while (local_ != nullptr && count < requests.size()) {
      auto* request = local_;
      local_ = request->next_;
      request->next_ = nullptr;
      requests[count] = request;
      transactions[count] = request->take_transaction();
      ++count;
    }
    const int group_error = orchfs_log_transaction_commit_group(
        transactions.data(), count, errors.data());
    if (group_error != 0) {
      for (std::size_t index = 0; index < count; ++index) {
        errors[index] = orchfs_log_transaction_commit(transactions[index]);
      }
    }
    for (std::size_t index = 0; index < count; ++index) {
      requests[index]->complete_on_owner(errors[index]);
    }
    return Runtime::PollState::progress;
  }

  Runtime* runtime_{};
  Runtime::PollRegistration registration_;
  std::atomic<CommitAwaiter*> inbox_{nullptr};
  CommitAwaiter* local_{};
};

}  // namespace

class KfsCoroutineCore::Impl {
  using RangeMap =
      std::unordered_map<InodeNumber, std::shared_ptr<RangeArbiter>>;
  using SnapshotMap = std::unordered_map<InodeNumber, FileStat>;
  static constexpr std::size_t kExtentCacheEntries = 8192;

 public:
  explicit Impl(Runtime& runtime)
      : runtime_(&runtime), device_(runtime), journal_(runtime) {}

  Task<Result<void>> schedule_inode_owner(InodeNumber inode) {
    if (inode < 0) {
      co_return errno_failure<void>(EINVAL);
    }
    const auto owner = runtime_->owner_for(static_cast<std::uint64_t>(inode));
    // Call schedule_on even when already on owner: it marks this nested frame
    // as owner-pinned for any later contended range handoff.
    co_return co_await runtime_->schedule_on(owner);
  }

  Task<Result<void>> schedule_namespace_owner() {
    // The same explicit ownership marker keeps namespace permits on worker 0.
    co_return co_await runtime_->schedule_on(0);
  }

  Result<std::shared_ptr<RangeArbiter>> range_for(InodeNumber inode) {
    auto current = ranges_.load(std::memory_order_acquire);
    for (;;) {
      if (const auto found = current->find(inode); found != current->end()) {
        return Result<std::shared_ptr<RangeArbiter>>::success(found->second);
      }
      try {
        auto updated = std::make_shared<RangeMap>(*current);
        auto range = std::make_shared<RangeArbiter>();
        updated->emplace(inode, range);
        std::shared_ptr<const RangeMap> published = std::move(updated);
        if (ranges_.compare_exchange_weak(
                current, std::move(published), std::memory_order_release,
                std::memory_order_acquire)) {
          return Result<std::shared_ptr<RangeArbiter>>::success(
              std::move(range));
        }
      } catch (const std::bad_alloc&) {
        return Result<std::shared_ptr<RangeArbiter>>::failure(
            std::make_error_code(std::errc::not_enough_memory));
      }
    }
  }

  void publish(const orchfs_core_inode& inode) {
    auto current = snapshots_.load(std::memory_order_acquire);
    for (;;) {
      auto updated = std::make_shared<SnapshotMap>(*current);
      (*updated)[inode.inode] = file_stat(inode);
      std::shared_ptr<const SnapshotMap> published = std::move(updated);
      if (snapshots_.compare_exchange_weak(
              current, std::move(published), std::memory_order_release,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  void erase_snapshot(InodeNumber inode) {
    erase_extents(inode);
    auto current = snapshots_.load(std::memory_order_acquire);
    for (;;) {
      if (!current->contains(inode)) {
        return;
      }
      auto updated = std::make_shared<SnapshotMap>(*current);
      updated->erase(inode);
      std::shared_ptr<const SnapshotMap> published = std::move(updated);
      if (snapshots_.compare_exchange_weak(
              current, std::move(published), std::memory_order_release,
              std::memory_order_acquire)) {
        return;
      }
    }
  }

  Result<FileStat> cached(InodeNumber inode) const noexcept {
    const auto snapshots = snapshots_.load(std::memory_order_acquire);
    const auto found = snapshots->find(inode);
    return found == snapshots->end()
               ? Result<FileStat>::failure(
                     std::make_error_code(std::errc::no_such_file_or_directory))
               : Result<FileStat>::success(found->second);
  }

  static std::size_t extent_cache_index(InodeNumber inode) noexcept {
    const auto key = static_cast<std::uint64_t>(inode);
    return static_cast<std::size_t>(detail::fmix64<false>(key)) &
           (kExtentCacheEntries - 1U);
  }

  ExtentCacheEntry* find_extent_entry(InodeNumber inode,
                                      bool create) noexcept {
    if (inode < 0) {
      return nullptr;
    }
    const std::size_t start = extent_cache_index(inode);
    for (std::size_t probe = 0; probe < kExtentCacheEntries; ++probe) {
      auto& entry = extent_cache_[(start + probe) &
                                  (kExtentCacheEntries - 1U)];
      InodeNumber found = entry.inode.load(std::memory_order_acquire);
      if (found == inode) {
        return &entry;
      }
      if (found != -1) {
        continue;
      }
      if (!create) {
        return nullptr;
      }
      InodeNumber empty = -1;
      if (entry.inode.compare_exchange_strong(
              empty, inode, std::memory_order_acq_rel,
              std::memory_order_acquire)) {
        return &entry;
      }
      if (empty == inode) {
        return &entry;
      }
    }
    return nullptr;
  }

  Result<std::shared_ptr<ExtentSnapshot>> build_extent_snapshot(
      InodeNumber inode, const orchfs_core_inode& metadata) {
    if (metadata.size < 0) {
      return errno_failure<std::shared_ptr<ExtentSnapshot>>(EIO);
    }
    try {
      auto snapshot = std::make_shared<ExtentSnapshot>();
      snapshot->file_size = static_cast<std::uint64_t>(metadata.size);
      const std::uint64_t block_count =
          (snapshot->file_size + kBlockSize - 1U) / kBlockSize;
      snapshot->extents.reserve(static_cast<std::size_t>(
          std::min<std::uint64_t>(block_count, 256U)));
      for (std::uint64_t block = 0; block < block_count; ++block) {
        orchfs_core_block mapping{};
        const int error = orchfs_core_query_block(
            inode, static_cast<std::int64_t>(block), &mapping);
        if (error != 0) {
          return errno_failure<std::shared_ptr<ExtentSnapshot>>(error);
        }
        append_extent(snapshot->extents, block, mapping);
      }
      return Result<std::shared_ptr<ExtentSnapshot>>::success(
          std::move(snapshot));
    } catch (const std::bad_alloc&) {
      return errno_failure<std::shared_ptr<ExtentSnapshot>>(ENOMEM);
    }
  }

  std::shared_ptr<const ExtentSnapshot> publish_extent_snapshot(
      InodeNumber inode, std::shared_ptr<ExtentSnapshot> snapshot) noexcept {
    auto* entry = find_extent_entry(inode, true);
    if (entry == nullptr) {
      return std::move(snapshot);
    }
    snapshot->generation =
        entry->next_generation.fetch_add(1, std::memory_order_relaxed);
    std::shared_ptr<const ExtentSnapshot> published = std::move(snapshot);
    entry->snapshot.store(published, std::memory_order_release);
    return published;
  }

  Result<std::shared_ptr<const ExtentSnapshot>> extents_for(
      InodeNumber inode, const orchfs_core_inode& metadata) {
    auto* entry = find_extent_entry(inode, true);
    if (entry != nullptr) {
      auto cached = entry->snapshot.load(std::memory_order_acquire);
      if (cached != nullptr && metadata.size >= 0 &&
          cached->file_size == static_cast<std::uint64_t>(metadata.size)) {
        return Result<std::shared_ptr<const ExtentSnapshot>>::success(
            std::move(cached));
      }
    }
    auto built = build_extent_snapshot(inode, metadata);
    if (!built) {
      return Result<std::shared_ptr<const ExtentSnapshot>>::failure(
          built.error());
    }
    return Result<std::shared_ptr<const ExtentSnapshot>>::success(
        publish_extent_snapshot(inode, std::move(built).value()));
  }

  void refresh_extents(InodeNumber inode) noexcept {
    orchfs_core_inode metadata{};
    if (orchfs_core_snapshot(inode, &metadata) != 0 ||
        metadata.type != ORCHFS_CORE_REGULAR) {
      erase_extents(inode);
      return;
    }
    auto built = build_extent_snapshot(inode, metadata);
    if (!built) {
      erase_extents(inode);
      return;
    }
    (void)publish_extent_snapshot(inode, std::move(built).value());
  }

  void refresh_extent_range(InodeNumber inode, std::uint64_t offset,
                            std::uint64_t length) noexcept {
    if (length == 0 || offset > std::numeric_limits<std::uint64_t>::max() -
                                    length) {
      return;
    }
    orchfs_core_inode metadata{};
    if (orchfs_core_snapshot(inode, &metadata) != 0 || metadata.size < 0 ||
        metadata.type != ORCHFS_CORE_REGULAR) {
      erase_extents(inode);
      return;
    }
    const std::uint64_t first_block = offset / kBlockSize;
    const std::uint64_t last_block = (offset + length - 1U) / kBlockSize;
    try {
      std::vector<orchfs_core_block> mappings(
          static_cast<std::size_t>(last_block - first_block + 1U));
      for (std::size_t index = 0; index < mappings.size(); ++index) {
        if (orchfs_core_query_block(
                inode, static_cast<std::int64_t>(first_block + index),
                &mappings[index]) != 0) {
          erase_extents(inode);
          return;
        }
      }
      const auto size = static_cast<std::uint64_t>(metadata.size);
      publish_extent_update(inode, size, size, first_block, mappings);
    } catch (const std::bad_alloc&) {
      erase_extents(inode);
    }
  }

  void publish_extent_update(
      InodeNumber inode, std::uint64_t old_size, std::uint64_t new_size,
      std::uint64_t first_block,
      std::span<const orchfs_core_block> replacements) noexcept {
    auto* entry = find_extent_entry(inode, true);
    if (entry == nullptr) {
      return;
    }
    const auto current = entry->snapshot.load(std::memory_order_acquire);
    if (current == nullptr || current->file_size != old_size ||
        first_block > std::numeric_limits<std::uint64_t>::max() -
                          replacements.size()) {
      refresh_extents(inode);
      return;
    }

    try {
      const std::uint64_t replace_end = first_block + replacements.size();
      const std::uint64_t new_block_count =
          (new_size + kBlockSize - 1U) / kBlockSize;
      std::vector<CachedExtent> candidates;
      candidates.reserve(current->extents.size() + replacements.size() + 2U);
      for (const auto& extent : current->extents) {
        const std::uint64_t extent_end =
            extent.first_block + extent.block_count;
        if (replacements.empty() || extent_end <= first_block ||
            extent.first_block >= replace_end) {
          candidates.push_back(extent);
          continue;
        }
        if (extent.first_block < first_block) {
          auto prefix = extent;
          prefix.block_count = first_block - extent.first_block;
          candidates.push_back(std::move(prefix));
        }
        if (extent_end > replace_end) {
          auto suffix = extent;
          const std::uint64_t delta = replace_end - extent.first_block;
          suffix.first_block = replace_end;
          suffix.block_count = extent_end - replace_end;
          if (suffix.mapping.type == ORCHFS_CORE_SSD_BLOCK) {
            suffix.mapping.ssd_device_offset +=
                static_cast<std::int64_t>(delta * kBlockSize);
            suffix.mapping.file_block += static_cast<std::int64_t>(delta);
          }
          candidates.push_back(std::move(suffix));
        }
      }
      for (std::size_t index = 0; index < replacements.size(); ++index) {
        candidates.push_back(CachedExtent{
            .first_block = first_block + index,
            .block_count = 1,
            .mapping = replacements[index],
        });
      }
      std::sort(candidates.begin(), candidates.end(),
                [](const CachedExtent& left, const CachedExtent& right) {
                  return left.first_block < right.first_block;
                });

      auto updated = std::make_shared<ExtentSnapshot>();
      updated->file_size = new_size;
      updated->extents.reserve(candidates.size());
      for (auto extent : candidates) {
        if (extent.first_block >= new_block_count) {
          continue;
        }
        extent.block_count = std::min(
            extent.block_count, new_block_count - extent.first_block);
        append_cached_extent(updated->extents, std::move(extent));
      }
      (void)publish_extent_snapshot(inode, std::move(updated));
    } catch (const std::bad_alloc&) {
      erase_extents(inode);
    }
  }

  void erase_extents(InodeNumber inode) noexcept {
    if (auto* entry = find_extent_entry(inode, false); entry != nullptr) {
      entry->snapshot.store({}, std::memory_order_release);
    }
  }

  Result<void> retain(InodeNumber inode) {
    try {
      ++open_references_[inode];
      return Result<void>::success();
    } catch (const std::bad_alloc&) {
      return errno_failure<void>(ENOMEM);
    }
  }

  Task<Result<void>> release(OpenedNode node) {
    ORCHFS_TRYV(co_await schedule_namespace_owner());
    auto found = open_references_.find(node.inode);
    if (found == open_references_.end() || found->second == 0) {
      co_return errno_failure<void>(EBADF);
    }
    if (found->second > 1) {
      --found->second;
      co_return Result<void>::success();
    }
    if (!orphaned_.contains(node.inode)) {
      open_references_.erase(found);
      co_return Result<void>::success();
    }

    ORCHFS_TRY(transaction, LogTransaction::create());
    const int error = transaction->invoke(
        [&] { return orchfs_core_delete_inode(node.inode); });
    if (error != 0) {
      co_return errno_failure<void>(error);
    }
    ORCHFS_TRYV(co_await journal_.commit(transaction->release()));
    open_references_.erase(node.inode);
    orphaned_.erase(node.inode);
    erase_snapshot(node.inode);
    co_return Result<void>::success();
  }

  struct NamespaceEntry {
    orchfs_core_inode inode{};
    InodeNumber parent{-1};
    std::uint64_t offset{};
    orchfs_core_dirent entry{};
  };

  struct ParentPath {
    InodeNumber parent{-1};
    std::string name;
  };

  Result<std::vector<std::string>> split_path(std::string_view path) {
    if (path.empty()) {
      return errno_failure<std::vector<std::string>>(ENOENT);
    }
    std::vector<std::string> components;
    try {
      std::size_t cursor = 0;
      while (cursor < path.size()) {
        while (cursor < path.size() && path[cursor] == '/') {
          ++cursor;
        }
        if (cursor == path.size()) {
          break;
        }
        const std::size_t end = path.find('/', cursor);
        const std::size_t length =
            (end == std::string_view::npos ? path.size() : end) - cursor;
        if (length > ORCHFS_CORE_DIRENT_NAME_MAX) {
          return errno_failure<std::vector<std::string>>(ENAMETOOLONG);
        }
        components.emplace_back(path.substr(cursor, length));
        cursor = end == std::string_view::npos ? path.size() : end + 1;
      }
    } catch (const std::bad_alloc&) {
      return errno_failure<std::vector<std::string>>(ENOMEM);
    }
    return Result<std::vector<std::string>>::success(std::move(components));
  }

  Task<Result<NamespaceEntry>> find_child(InodeNumber directory,
                                           std::string_view name) {
    orchfs_core_inode parent{};
    int error = orchfs_core_snapshot(directory, &parent);
    if (error != 0) {
      co_return errno_failure<NamespaceEntry>(error);
    }
    if (parent.type != ORCHFS_CORE_DIRECTORY) {
      co_return errno_failure<NamespaceEntry>(ENOTDIR);
    }
    if (parent.size < 0 ||
        parent.size % ORCHFS_CORE_DIRENT_SIZE != 0) {
      co_return errno_failure<NamespaceEntry>(EIO);
    }

    constexpr std::size_t kWindowBytes =
        kDirectoryWindowEntries * ORCHFS_CORE_DIRENT_SIZE;
    std::array<std::byte, kWindowBytes> window{};
    for (std::uint64_t cursor = 0;
         cursor < static_cast<std::uint64_t>(parent.size);) {
      const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(
          window.size(), static_cast<std::uint64_t>(parent.size) - cursor));
      ORCHFS_TRY(bytes_read, co_await read_unlocked(
          directory, cursor, std::span<std::byte>(window).first(length)));
      if (bytes_read != length) {
        co_return errno_failure<NamespaceEntry>(EIO);
      }
      for (std::size_t inside = 0; inside < length;
           inside += ORCHFS_CORE_DIRENT_SIZE) {
        orchfs_core_dirent entry{};
        std::memcpy(&entry, window.data() + inside, sizeof(entry));
        if (entry.type == 0 ||
            entry.name_length > ORCHFS_CORE_DIRENT_NAME_MAX) {
          continue;
        }
        if (entry.name_length == name.size() &&
            std::memcmp(entry.name, name.data(), name.size()) == 0) {
          orchfs_core_inode child{};
          error = orchfs_core_snapshot(entry.inode, &child);
          if (error != 0) {
            co_return errno_failure<NamespaceEntry>(error);
          }
          co_return Result<NamespaceEntry>::success(NamespaceEntry{
              .inode = child,
              .parent = directory,
              .offset = cursor + inside,
              .entry = entry,
          });
        }
      }
      cursor += length;
    }
    co_return errno_failure<NamespaceEntry>(ENOENT);
  }

  Task<Result<NamespaceEntry>> resolve(
      std::string_view path, std::optional<InodeNumber> relative_to = {}) {
    ORCHFS_TRY(components, split_path(path));
    InodeNumber current =
        !path.empty() && path.front() != '/' && relative_to
            ? *relative_to
            : orchfs_core_root_inode();
    if (current < 0) {
      co_return errno_failure<NamespaceEntry>(EIO);
    }
    NamespaceEntry located{};
    located.inode.inode = current;
    int error = orchfs_core_snapshot(current, &located.inode);
    if (error != 0) {
      co_return errno_failure<NamespaceEntry>(error);
    }
    for (const auto& component : components) {
      if (component == ".") {
        continue;
      }
      ORCHFS_TRY(child, co_await find_child(current, component));
      located = std::move(child);
      current = located.inode.inode;
    }
    co_return Result<NamespaceEntry>::success(std::move(located));
  }

  Task<Result<ParentPath>> resolve_parent(
      std::string_view path, std::optional<InodeNumber> relative_to = {}) {
    ORCHFS_TRY(components, split_path(path));
    if (components.empty()) {
      co_return errno_failure<ParentPath>(EBUSY);
    }
    std::string name = std::move(components.back());
    components.pop_back();
    if (name.empty() || name == "." || name == "..") {
      co_return errno_failure<ParentPath>(EINVAL);
    }
    InodeNumber current =
        !path.empty() && path.front() != '/' && relative_to
            ? *relative_to
            : orchfs_core_root_inode();
    for (const auto& component : components) {
      if (component == ".") {
        continue;
      }
      ORCHFS_TRY(child, co_await find_child(current, component));
      if (child.inode.type != ORCHFS_CORE_DIRECTORY) {
        co_return errno_failure<ParentPath>(ENOTDIR);
      }
      current = child.inode.inode;
    }
    co_return Result<ParentPath>::success(
        ParentPath{.parent = current, .name = std::move(name)});
  }

  Task<Result<void>> write_directory_entry(
      InodeNumber directory, std::uint64_t offset,
      const orchfs_core_dirent& entry,
      LogTransaction* transaction = nullptr) {
    auto bytes = std::as_bytes(std::span(&entry, 1));
    ORCHFS_TRY(written, co_await write_unlocked(
        directory, offset, bytes, false, true, transaction));
    co_return written.bytes == sizeof(entry)
                  ? Result<void>::success()
                  : errno_failure<void>(EIO);
  }

  Task<Result<std::uint64_t>> free_directory_slot(InodeNumber directory) {
    orchfs_core_inode snapshot{};
    const int error = orchfs_core_snapshot(directory, &snapshot);
    if (error != 0) {
      co_return errno_failure<std::uint64_t>(error);
    }
    if (snapshot.type != ORCHFS_CORE_DIRECTORY || snapshot.size < 0 ||
        snapshot.size % ORCHFS_CORE_DIRENT_SIZE != 0) {
      co_return errno_failure<std::uint64_t>(ENOTDIR);
    }
    std::array<std::byte, ORCHFS_CORE_DIRENT_SIZE> bytes{};
    for (std::uint64_t cursor = 2 * ORCHFS_CORE_DIRENT_SIZE;
         cursor < static_cast<std::uint64_t>(snapshot.size);
         cursor += ORCHFS_CORE_DIRENT_SIZE) {
      ORCHFS_TRYV(co_await read_unlocked(directory, cursor, bytes));
      orchfs_core_dirent entry{};
      std::memcpy(&entry, bytes.data(), sizeof(entry));
      if (entry.type == 0) {
        co_return Result<std::uint64_t>::success(cursor);
      }
    }
    co_return Result<std::uint64_t>::success(
        static_cast<std::uint64_t>(snapshot.size));
  }

  Task<Result<NamespaceEntry>> create_node(InodeNumber parent,
                                            std::string name, int type,
                                            std::uint32_t mode) {
    auto existing = co_await find_child(parent, name);
    if (existing) {
      co_return errno_failure<NamespaceEntry>(EEXIST);
    }
    if (existing.error().value() != ENOENT) {
      co_return Result<NamespaceEntry>::failure(existing.error());
    }

    ORCHFS_TRY(transaction, LogTransaction::create());

    orchfs_core_inode inode{};
    int error = transaction->invoke(
        [&] { return orchfs_core_create_inode(type, mode, &inode); });
    if (error != 0) {
      co_return errno_failure<NamespaceEntry>(error);
    }
    if (type == ORCHFS_CORE_DIRECTORY) {
      std::array<orchfs_core_dirent, 2> initial{};
      initial[0].inode = inode.inode;
      initial[0].offset = 0;
      initial[0].name_length = 1;
      initial[0].type = ORCHFS_CORE_DIRECTORY;
      std::memcpy(initial[0].name, ".", 1);
      initial[1].inode = parent;
      initial[1].offset = ORCHFS_CORE_DIRENT_SIZE;
      initial[1].name_length = 2;
      initial[1].type = ORCHFS_CORE_DIRECTORY;
      std::memcpy(initial[1].name, "..", 2);
      ORCHFS_TRY(initialized, co_await write_unlocked(
          inode.inode, 0, std::as_bytes(std::span(initial)), false, true,
          transaction.get()));
      if (initialized.bytes != sizeof(initial)) {
        co_return errno_failure<NamespaceEntry>(EIO);
      }
    }

    ORCHFS_TRY(slot, co_await free_directory_slot(parent));
    orchfs_core_dirent entry{};
    entry.inode = inode.inode;
    entry.offset = static_cast<std::int64_t>(slot);
    entry.name_length = static_cast<std::uint16_t>(name.size());
    entry.type = static_cast<std::uint8_t>(type);
    std::memcpy(entry.name, name.data(), name.size());
    ORCHFS_TRYV(co_await write_directory_entry(
        parent, slot, entry, transaction.get()));
    ORCHFS_TRYV(co_await journal_.commit(transaction->release()));
    (void)orchfs_core_snapshot(inode.inode, &inode);
    publish(inode);
    orchfs_core_inode parent_snapshot{};
    if (orchfs_core_snapshot(parent, &parent_snapshot) == 0) {
      publish(parent_snapshot);
    }
    co_return Result<NamespaceEntry>::success(NamespaceEntry{
        .inode = inode,
        .parent = parent,
        .offset = slot,
        .entry = entry,
    });
  }

  Task<Result<OpenedNode>> open_path(
      std::string_view path, int flags, std::uint32_t mode,
      std::optional<InodeNumber> relative_to = {}) {
    auto located = co_await resolve(path, relative_to);
    if (located && (flags & O_CREAT) != 0 && (flags & O_EXCL) != 0) {
      co_return errno_failure<OpenedNode>(EEXIST);
    }
    if (!located) {
      if (located.error().value() != ENOENT || (flags & O_CREAT) == 0) {
        co_return Result<OpenedNode>::failure(located.error());
      }
      ORCHFS_TRY(parent, co_await resolve_parent(path, relative_to));
      located = co_await create_node(parent.parent,
                                     std::move(parent.name),
                                     ORCHFS_CORE_REGULAR, mode);
      if (!located) {
        co_return Result<OpenedNode>::failure(located.error());
      }
    }
    if ((flags & O_DIRECTORY) != 0 &&
        located.value().inode.type != ORCHFS_CORE_DIRECTORY) {
      co_return errno_failure<OpenedNode>(ENOTDIR);
    }
    if ((flags & O_TRUNC) != 0) {
      if (located.value().inode.type != ORCHFS_CORE_REGULAR) {
        co_return errno_failure<OpenedNode>(EISDIR);
      }
      if ((flags & O_ACCMODE) == O_RDONLY) {
        co_return errno_failure<OpenedNode>(EINVAL);
      }
      ORCHFS_TRY(range, range_for(located.value().inode.inode));
      ORCHFS_TRY(range_permit, co_await range->acquire(
          0, kWholeFile, RangeMode::write));
      const std::uint64_t old_extent_size = located.value().inode.size > 0
          ? static_cast<std::uint64_t>(located.value().inode.size)
          : 0;
      auto transaction_result = LogTransaction::create();
      if (!transaction_result) {
        auto released = co_await range_permit.release();
        (void)released;
        co_return Result<OpenedNode>::failure(transaction_result.error());
      }
      auto transaction = std::move(transaction_result).value();
      const int error = transaction->invoke([&] {
        return orchfs_core_set_size(located.value().inode.inode, 0);
      });
      Result<void> committed = error == 0
          ? co_await journal_.commit(transaction->release())
          : errno_failure<void>(error);
      auto released = co_await range_permit.release();
      if (error != 0) {
        co_return errno_failure<OpenedNode>(error);
      }
      if (!committed) {
        co_return Result<OpenedNode>::failure(committed.error());
      }
      if (!released) {
        co_return Result<OpenedNode>::failure(released.error());
      }
      if (orchfs_core_snapshot(located.value().inode.inode,
                               &located.value().inode) == 0 &&
          located.value().inode.size >= 0) {
        publish_extent_update(
            located.value().inode.inode, old_extent_size,
            static_cast<std::uint64_t>(located.value().inode.size), 0, {});
      } else {
        erase_extents(located.value().inode.inode);
      }
    }
    publish(located.value().inode);
    const auto node = opened_node(located.value().inode);
    if (node.type == NodeType::unknown) {
      co_return errno_failure<OpenedNode>(EIO);
    }
    co_return Result<OpenedNode>::success(node);
  }

  Task<Result<bool>> directory_empty(InodeNumber inode) {
    orchfs_core_inode snapshot{};
    const int error = orchfs_core_snapshot(inode, &snapshot);
    if (error != 0) {
      co_return errno_failure<bool>(error);
    }
    if (snapshot.type != ORCHFS_CORE_DIRECTORY || snapshot.size < 0 ||
        snapshot.size % ORCHFS_CORE_DIRENT_SIZE != 0) {
      co_return errno_failure<bool>(ENOTDIR);
    }
    std::array<std::byte, ORCHFS_CORE_DIRENT_SIZE> bytes{};
    for (std::uint64_t cursor = 2 * ORCHFS_CORE_DIRENT_SIZE;
         cursor < static_cast<std::uint64_t>(snapshot.size);
         cursor += ORCHFS_CORE_DIRENT_SIZE) {
      ORCHFS_TRYV(co_await read_unlocked(inode, cursor, bytes));
      orchfs_core_dirent entry{};
      std::memcpy(&entry, bytes.data(), sizeof(entry));
      if (entry.type != 0) {
        co_return Result<bool>::success(false);
      }
    }
    co_return Result<bool>::success(true);
  }

  Task<Result<void>> remove_path(std::string_view path, int expected_type) {
    ORCHFS_TRY(parent, co_await resolve_parent(path));
    ORCHFS_TRY(child, co_await find_child(parent.parent, parent.name));
    if (child.inode.inode == orchfs_core_root_inode()) {
      co_return errno_failure<void>(EBUSY);
    }
    if (child.inode.type != expected_type) {
      co_return errno_failure<void>(
          expected_type == ORCHFS_CORE_DIRECTORY ? ENOTDIR : EISDIR);
    }
    if (expected_type == ORCHFS_CORE_DIRECTORY) {
      ORCHFS_TRY(empty, co_await directory_empty(child.inode.inode));
      if (!empty) {
        co_return errno_failure<void>(ENOTEMPTY);
      }
    }
    child.entry.type = 0;
    ORCHFS_TRY(transaction, LogTransaction::create());
    const bool defer_delete = open_references_.contains(
        child.inode.inode);
    if (defer_delete) {
      try {
        orphaned_.insert(child.inode.inode);
      } catch (const std::bad_alloc&) {
        co_return errno_failure<void>(ENOMEM);
      }
    }
    auto removed = co_await write_directory_entry(
        child.parent, child.offset, child.entry,
        transaction.get());
    if (!removed) {
      if (defer_delete) {
        orphaned_.erase(child.inode.inode);
      }
      co_return removed;
    }
    if (!defer_delete) {
      const int error = transaction->invoke([&] {
        return orchfs_core_delete_inode(child.inode.inode);
      });
      if (error != 0) {
        co_return errno_failure<void>(error);
      }
    }
    auto committed = co_await journal_.commit(transaction->release());
    if (!committed) {
      if (defer_delete) {
        orphaned_.erase(child.inode.inode);
      }
      co_return committed;
    }
    if (!defer_delete) {
      erase_snapshot(child.inode.inode);
    }
    orchfs_core_inode parent_snapshot{};
    if (orchfs_core_snapshot(child.parent, &parent_snapshot) == 0) {
      publish(parent_snapshot);
    }
    co_return Result<void>::success();
  }

  Task<Result<void>> rename_path(std::string_view old_path,
                                 std::string_view new_path) {
    ORCHFS_TRY(old_parent, co_await resolve_parent(old_path));
    ORCHFS_TRY(new_parent, co_await resolve_parent(new_path));
    if (old_parent.parent != new_parent.parent) {
      co_return errno_failure<void>(EXDEV);
    }
    ORCHFS_TRY(source,
               co_await find_child(old_parent.parent, old_parent.name));
    if (old_parent.name == new_parent.name) {
      co_return Result<void>::success();
    }
    auto destination = co_await find_child(new_parent.parent,
                                           new_parent.name);
    if (destination) {
      co_return errno_failure<void>(EEXIST);
    }
    if (destination.error().value() != ENOENT) {
      co_return Result<void>::failure(destination.error());
    }
    auto& entry = source.entry;
    entry.name_length =
        static_cast<std::uint16_t>(new_parent.name.size());
    std::memset(entry.name, 0, sizeof(entry.name));
    std::memcpy(entry.name, new_parent.name.data(), new_parent.name.size());
    co_return co_await write_directory_entry(
        source.parent, source.offset, entry);
  }

  Task<Result<std::size_t>> read_unlocked(
      InodeNumber inode, std::uint64_t offset,
      std::span<std::byte> destination) {
    orchfs_core_inode snapshot{};
    int error = orchfs_core_snapshot(inode, &snapshot);
    if (error != 0) {
      co_return errno_failure<std::size_t>(error);
    }
    if (snapshot.type != ORCHFS_CORE_REGULAR &&
        snapshot.type != ORCHFS_CORE_DIRECTORY) {
      co_return errno_failure<std::size_t>(EIO);
    }
    if (snapshot.size < 0) {
      co_return errno_failure<std::size_t>(EIO);
    }
    if (offset >= static_cast<std::uint64_t>(snapshot.size) ||
        destination.empty()) {
      co_return Result<std::size_t>::success(0);
    }
    const std::size_t length = static_cast<std::size_t>(std::min<std::uint64_t>(
        destination.size(), static_cast<std::uint64_t>(snapshot.size) - offset));

    std::shared_ptr<const ExtentSnapshot> extent_snapshot;
    if (snapshot.type == ORCHFS_CORE_REGULAR) {
      ORCHFS_TRY(cached_extents, extents_for(inode, snapshot));
      extent_snapshot = std::move(cached_extents);
    }

    const std::uint64_t first_block = offset / kBlockSize;
    const std::uint64_t last_block = (offset + length - 1) / kBlockSize;
    boost::container::small_vector<ReadScratch, 2> scratch;
    boost::container::small_vector<BlockRead, 8> requests;
    try {
      scratch.reserve(static_cast<std::size_t>(last_block - first_block + 1));
      requests.reserve(static_cast<std::size_t>(last_block - first_block + 1));
    } catch (const std::bad_alloc&) {
      co_return errno_failure<std::size_t>(ENOMEM);
    }

    std::size_t consumed = 0;
    std::size_t copied_since_yield = 0;
    for (std::uint64_t block_index = first_block;
         block_index <= last_block; ++block_index) {
      orchfs_core_block block{};
      if (extent_snapshot != nullptr) {
        if (!mapping_from_snapshot(*extent_snapshot, block_index, block)) {
          co_return errno_failure<std::size_t>(EIO);
        }
      } else {
        error = orchfs_core_query_block(
            inode, static_cast<std::int64_t>(block_index), &block);
        if (error != 0) {
          co_return errno_failure<std::size_t>(error);
        }
      }
      const std::uint64_t logical_start = block_index * kBlockSize;
      const std::size_t inside = static_cast<std::size_t>(
          std::max(offset, logical_start) - logical_start);
      const std::size_t chunk = static_cast<std::size_t>(std::min<std::uint64_t>(
          kBlockSize - inside, length - consumed));
      std::byte* output = destination.data() + consumed;

      if (block.type == ORCHFS_CORE_VIRTUAL_BLOCK) {
        std::size_t page_consumed = 0;
        while (page_consumed < chunk) {
          const std::size_t position = inside + page_consumed;
          const std::size_t page = position / kPageSize;
          const std::size_t in_page = position % kPageSize;
          const std::size_t part = std::min<std::size_t>(
              chunk - page_consumed, kPageSize - in_page);
          if (page >= ORCHFS_CORE_PAGE_COUNT ||
              block.nvm_page_offset[page] < 0) {
            co_return errno_failure<std::size_t>(EIO);
          }
          error = orchfs_core_read_pmem(
              block.nvm_page_offset[page] + static_cast<std::int64_t>(in_page),
              output + page_consumed, part);
          if (error != 0) {
            co_return errno_failure<std::size_t>(error);
          }
          page_consumed += part;
          copied_since_yield += part;
          if (copied_since_yield >= kYieldBytes) {
            ORCHFS_TRYV(co_await Runtime::yield());
            copied_since_yield = 0;
          }
        }
      } else if (block.type == ORCHFS_CORE_SSD_BLOCK && inside == 0 &&
                 chunk == kBlockSize) {
        requests.push_back(BlockRead{
            .offset = static_cast<std::uint64_t>(block.ssd_device_offset),
            .destination = std::span<std::byte>(output, chunk),
        });
      } else if (block.type == ORCHFS_CORE_SSD_BLOCK ||
                 block.type == ORCHFS_CORE_STRATA_BLOCK) {
        try {
          scratch.push_back(ReadScratch{
              .block = std::vector<std::byte>(kBlockSize),
              .destination = output,
              .block_offset = inside,
              .length = chunk,
              .mapping = block,
              .strata = block.type == ORCHFS_CORE_STRATA_BLOCK,
          });
        } catch (const std::bad_alloc&) {
          co_return errno_failure<std::size_t>(ENOMEM);
        }
        auto& item = scratch.back();
        if (block.ssd_device_offset < 0) {
          co_return errno_failure<std::size_t>(EIO);
        }
        requests.push_back(BlockRead{
            .offset = static_cast<std::uint64_t>(block.ssd_device_offset),
            .destination = item.block,
        });
      } else {
        co_return errno_failure<std::size_t>(EIO);
      }
      consumed += chunk;
    }

    if (!requests.empty()) {
      ORCHFS_TRY(bytes_read, co_await device_.read_batch(
          std::span<const BlockRead>(requests.data(), requests.size())));
      std::size_t expected = 0;
      for (const auto& request : requests) {
        expected += request.destination.size();
      }
      if (bytes_read != expected) {
        co_return errno_failure<std::size_t>(EIO);
      }
    }

    for (auto& item : scratch) {
      if (item.strata) {
        for (std::size_t page = 0; page < ORCHFS_CORE_PAGE_COUNT; ++page) {
          if (item.mapping.nvm_page_offset[page] < 0) {
            continue;
          }
          std::array<std::int16_t, kBufferMetadataEntries> metadata{};
          error = read_metadata(item.mapping.buffer_metadata_offset[page],
                                metadata);
          if (error != 0) {
            co_return errno_failure<std::size_t>(error);
          }
          error = overlay_page(
              std::span<std::byte>(item.block).subspan(page * kPageSize,
                                                        kPageSize),
              item.mapping.nvm_page_offset[page], metadata);
          if (error != 0) {
            co_return errno_failure<std::size_t>(error);
          }
        }
      }
      std::memcpy(item.destination, item.block.data() + item.block_offset,
                  item.length);
      copied_since_yield += item.length;
      if (copied_since_yield >= kYieldBytes) {
        ORCHFS_TRYV(co_await Runtime::yield());
        copied_since_yield = 0;
      }
    }
    co_return Result<std::size_t>::success(length);
  }

  Task<Result<WriteResult>> write_unlocked(
      InodeNumber inode, std::uint64_t requested_offset,
      std::span<const std::byte> source, bool append,
      bool allow_directory = false,
      LogTransaction* external_transaction = nullptr) {
    orchfs_core_inode snapshot{};
    int error = orchfs_core_snapshot(inode, &snapshot);
    if (error != 0) {
      co_return errno_failure<WriteResult>(error);
    }
    if (snapshot.type != ORCHFS_CORE_REGULAR &&
        !(allow_directory && snapshot.type == ORCHFS_CORE_DIRECTORY)) {
      co_return errno_failure<WriteResult>(EISDIR);
    }
    if (snapshot.size < 0) {
      co_return errno_failure<WriteResult>(EIO);
    }
    const std::uint64_t current_size = static_cast<std::uint64_t>(snapshot.size);
    const std::uint64_t offset = append ? current_size : requested_offset;
    if (offset > current_size) {
      co_return Result<WriteResult>::success(
          WriteResult{.bytes = 0, .offset = offset});
    }
    if (source.empty()) {
      co_return Result<WriteResult>::success(
          WriteResult{.bytes = 0, .offset = offset});
    }
    if (offset > kMaximumFileSize ||
        source.size() > kMaximumFileSize - offset) {
      co_return errno_failure<WriteResult>(EFBIG);
    }

    const std::uint64_t end = offset + source.size();
    const std::uint64_t first_block = offset / kBlockSize;
    const std::uint64_t last_block = (end - 1) / kBlockSize;
    boost::container::small_vector<orchfs_core_block, 4> blocks;
    boost::container::small_vector<BlockRead, 4> merge_reads;
    boost::container::small_vector<BlockWrite, 4> writes;
    boost::container::small_vector<PmemWrite, 8> pmem_writes;
    boost::container::small_vector<MetadataWrite, 8> metadata_writes;
    boost::container::small_vector<MergePage, 1> merges;
    try {
      const auto block_count =
          static_cast<std::size_t>(last_block - first_block + 1);
      blocks.resize(block_count);
    } catch (const std::bad_alloc&) {
      co_return errno_failure<WriteResult>(ENOMEM);
    }

    std::unique_ptr<LogTransaction> owned_transaction;
    LogTransaction* transaction = external_transaction;
    bool stable_overwrite = transaction == nullptr && !allow_directory &&
                            !append && offset % kBlockSize == 0 &&
                            source.size() % kBlockSize == 0 &&
                            end <= current_size;
    if (stable_overwrite) {
      ORCHFS_TRY(extent_snapshot, extents_for(inode, snapshot));
      for (std::size_t index = 0; index < blocks.size(); ++index) {
        if (!mapping_from_snapshot(*extent_snapshot, first_block + index,
                                   blocks[index]) ||
            (blocks[index].type != ORCHFS_CORE_SSD_BLOCK &&
             blocks[index].type != ORCHFS_CORE_VIRTUAL_BLOCK)) {
          stable_overwrite = false;
          break;
        }
      }
    }
    if (!stable_overwrite) {
      if (transaction == nullptr) {
        ORCHFS_TRY(created, LogTransaction::create());
        owned_transaction = std::move(created);
        transaction = owned_transaction.get();
      }
      error = transaction->invoke([&] {
        return orchfs_core_prepare_write_blocks(
            inode, static_cast<std::int64_t>(first_block),
            static_cast<std::int64_t>(last_block),
            static_cast<std::int64_t>((end - 1) % kBlockSize), blocks.data());
      });
      if (error != 0) {
        co_return errno_failure<WriteResult>(error);
      }
    }

    std::size_t consumed = 0;
    for (std::size_t index = 0; index < blocks.size(); ++index) {
      auto& block = blocks[index];
      const std::uint64_t block_index = first_block + index;
      const std::uint64_t logical_start = block_index * kBlockSize;
      const std::size_t inside = static_cast<std::size_t>(
          std::max(offset, logical_start) - logical_start);
      const std::size_t chunk = std::min<std::size_t>(
          source.size() - consumed, kBlockSize - inside);
      const std::byte* input = source.data() + consumed;

      if (block.type == ORCHFS_CORE_SSD_BLOCK &&
          (inside != 0 || chunk != kBlockSize)) {
        error = transaction->invoke([&] {
          return orchfs_core_ensure_strata(
              inode, static_cast<std::int64_t>(block_index),
              static_cast<std::int64_t>(inside),
              static_cast<std::int64_t>(chunk), &block);
        });
        if (error != 0) {
          co_return errno_failure<WriteResult>(error);
        }
      }

      if (block.type == ORCHFS_CORE_SSD_BLOCK) {
        if (inside != 0 || chunk != kBlockSize ||
            block.ssd_device_offset < 0) {
          co_return errno_failure<WriteResult>(EIO);
        }
        writes.push_back(BlockWrite{
            .offset = static_cast<std::uint64_t>(block.ssd_device_offset),
            .source = std::span<const std::byte>(input, chunk),
        });
      } else if (block.type == ORCHFS_CORE_VIRTUAL_BLOCK) {
        std::size_t page_consumed = 0;
        while (page_consumed < chunk) {
          const std::size_t position = inside + page_consumed;
          const std::size_t page = position / kPageSize;
          const std::size_t in_page = position % kPageSize;
          const std::size_t part = std::min<std::size_t>(
              chunk - page_consumed, kPageSize - in_page);
          if (page >= ORCHFS_CORE_PAGE_COUNT ||
              block.nvm_page_offset[page] < 0) {
            co_return errno_failure<WriteResult>(EIO);
          }
          pmem_writes.push_back(PmemWrite{
              .offset = block.nvm_page_offset[page] +
                        static_cast<std::int64_t>(in_page),
              .source = input + page_consumed,
              .length = part,
          });
          page_consumed += part;
        }
      } else if (block.type == ORCHFS_CORE_STRATA_BLOCK) {
        if (block.ssd_device_offset < 0) {
          co_return errno_failure<WriteResult>(EIO);
        }
        if (inside == 0 && chunk == kBlockSize) {
          writes.push_back(BlockWrite{
              .offset = static_cast<std::uint64_t>(block.ssd_device_offset),
              .source = std::span<const std::byte>(input, chunk),
          });
          for (std::size_t page = 0; page < ORCHFS_CORE_PAGE_COUNT; ++page) {
            if (block.buffer_metadata_offset[page] >= 0) {
              metadata_writes.push_back(MetadataWrite{
                  .offset = block.buffer_metadata_offset[page],
              });
            }
          }
        } else {
          std::size_t page_consumed = 0;
          while (page_consumed < chunk) {
            const std::size_t position = inside + page_consumed;
            const std::size_t page = position / kPageSize;
            const std::size_t in_page = position % kPageSize;
            const std::size_t part = std::min<std::size_t>(
                chunk - page_consumed, kPageSize - in_page);
            if (page >= ORCHFS_CORE_PAGE_COUNT ||
                block.nvm_page_offset[page] < 0 ||
                block.buffer_metadata_offset[page] < 0) {
              co_return errno_failure<WriteResult>(EIO);
            }

            std::array<std::int16_t, kBufferMetadataEntries> metadata{};
            error = read_metadata(block.buffer_metadata_offset[page], metadata);
            if (error != 0) {
              co_return errno_failure<WriteResult>(error);
            }
            if (in_page == 0 && part == kPageSize) {
              writes.push_back(BlockWrite{
                  .offset = static_cast<std::uint64_t>(
                      block.ssd_device_offset +
                      static_cast<std::int64_t>(page * kPageSize)),
                  .source = std::span<const std::byte>(
                      input + page_consumed, part),
              });
              metadata.fill(0);
            } else {
              if (metadata[0] == kMaximumBufferSegments) {
                try {
                  merges.push_back(MergePage{
                      .page = std::vector<std::byte>(kPageSize),
                      .ssd_offset = block.ssd_device_offset +
                                    static_cast<std::int64_t>(page * kPageSize),
                      .nvm_offset = block.nvm_page_offset[page],
                      .metadata = metadata,
                  });
                } catch (const std::bad_alloc&) {
                  co_return errno_failure<WriteResult>(ENOMEM);
                }
                auto& merge = merges.back();
                merge_reads.push_back(BlockRead{
                    .offset = static_cast<std::uint64_t>(merge.ssd_offset),
                    .destination = merge.page,
                });
                metadata.fill(0);
              }
              const auto segment = metadata[0]++;
              metadata[1 + segment * 2] =
                  static_cast<std::int16_t>(in_page);
              metadata[2 + segment * 2] =
                  static_cast<std::int16_t>(part);
              pmem_writes.push_back(PmemWrite{
                  .offset = block.nvm_page_offset[page] +
                            static_cast<std::int64_t>(in_page),
                  .source = input + page_consumed,
                  .length = part,
              });
            }
            metadata_writes.push_back(MetadataWrite{
                .offset = block.buffer_metadata_offset[page],
                .value = metadata,
            });
            page_consumed += part;
          }
        }
      } else {
        co_return errno_failure<WriteResult>(EIO);
      }
      consumed += chunk;
    }

    if (!merge_reads.empty()) {
      ORCHFS_TRY(bytes_read, co_await device_.read_batch(
          std::span<const BlockRead>(merge_reads.data(), merge_reads.size())));
      if (bytes_read != merge_reads.size() * kPageSize) {
        co_return errno_failure<WriteResult>(EIO);
      }
      for (auto& merge : merges) {
        error = overlay_page(merge.page, merge.nvm_offset, merge.metadata);
        if (error != 0) {
          co_return errno_failure<WriteResult>(error);
        }
        writes.push_back(BlockWrite{
            .offset = static_cast<std::uint64_t>(merge.ssd_offset),
            .source = merge.page,
        });
      }
    }

    if (!writes.empty()) {
      ORCHFS_TRY(bytes_written, co_await device_.write_batch(
          std::span<const BlockWrite>(writes.data(), writes.size())));
      std::size_t expected = 0;
      for (const auto& request : writes) {
        expected += request.source.size();
      }
      if (bytes_written != expected) {
        co_return errno_failure<WriteResult>(EIO);
      }
    }

    std::size_t copied_since_yield = 0;
    for (const auto& operation : pmem_writes) {
      error = allow_directory
          ? transaction->add_pmem(operation.offset, operation.source,
                                  operation.length)
          : orchfs_core_write_pmem(operation.offset, operation.source,
                                   operation.length);
      if (error != 0) {
        co_return errno_failure<WriteResult>(error);
      }
      copied_since_yield += operation.length;
      if (copied_since_yield >= kYieldBytes) {
        ORCHFS_TRYV(co_await Runtime::yield());
        copied_since_yield = 0;
      }
    }
    for (const auto& operation : metadata_writes) {
      error = transaction->add_pmem(operation.offset, operation.value.data(),
                                    kBufferMetadataSize);
      if (error != 0) {
        co_return errno_failure<WriteResult>(error);
      }
    }

    if (end > current_size) {
      error = transaction->invoke(
          [&] { return orchfs_core_set_size(inode, end); });
      if (error != 0) {
        co_return errno_failure<WriteResult>(error);
      }
    }
    if (owned_transaction) {
      ORCHFS_TRYV(co_await journal_.commit(owned_transaction->release()));
      error = orchfs_core_snapshot(inode, &snapshot);
      if (error == 0) {
        publish(snapshot);
        if (!allow_directory && snapshot.type == ORCHFS_CORE_REGULAR &&
            snapshot.size >= 0) {
          publish_extent_update(
              inode, current_size, static_cast<std::uint64_t>(snapshot.size),
              first_block,
              std::span<const orchfs_core_block>(blocks.data(),
                                                 blocks.size()));
        }
      } else {
        erase_extents(inode);
      }
    }
    co_return Result<WriteResult>::success(
        WriteResult{.bytes = source.size(), .offset = offset});
  }

  Runtime* runtime_;
  AsyncBlockDevice device_;
  JournalService journal_;
  RangeArbiter namespace_gate_;
  std::atomic<std::shared_ptr<const RangeMap>> ranges_{
      std::make_shared<const RangeMap>()};
  std::atomic<std::shared_ptr<const SnapshotMap>> snapshots_{
      std::make_shared<const SnapshotMap>()};
  std::array<ExtentCacheEntry, kExtentCacheEntries> extent_cache_{};
  std::unordered_map<InodeNumber, std::size_t> open_references_;
  std::unordered_set<InodeNumber> orphaned_;
};

KfsCoroutineCore::KfsCoroutineCore(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {}

KfsCoroutineCore::~KfsCoroutineCore() = default;

Result<std::shared_ptr<KfsCoroutineCore>> KfsCoroutineCore::create(
    Runtime& runtime) {
  try {
    auto core = std::shared_ptr<KfsCoroutineCore>(
        new KfsCoroutineCore(std::make_unique<Impl>(runtime)));
    return Result<std::shared_ptr<KfsCoroutineCore>>::success(std::move(core));
  } catch (const std::bad_alloc&) {
    return Result<std::shared_ptr<KfsCoroutineCore>>::failure(
        std::make_error_code(std::errc::not_enough_memory));
  }
}

Task<Result<OpenedNode>> KfsCoroutineCore::open(
    std::string path, int flags, std::uint32_t mode) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  ORCHFS_TRY(permit, co_await impl_->namespace_gate_.acquire(
      0, 1, RangeMode::write));
  auto result = co_await impl_->open_path(path, flags, mode);
  if (result) {
    auto retained = impl_->retain(result.value().inode);
    if (!retained) {
      auto released = co_await permit.release();
      (void)released;
      co_return Result<OpenedNode>::failure(retained.error());
    }
  }
  auto released = co_await permit.release();
  if (!released && result) {
    auto dropped = co_await impl_->release(result.value());
    (void)dropped;
    co_return Result<OpenedNode>::failure(released.error());
  }
  co_return result;
}

Task<Result<OpenedNode>> KfsCoroutineCore::open_at(
    InodeNumber directory, std::string path, int flags, std::uint32_t mode) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  ORCHFS_TRY(permit, co_await impl_->namespace_gate_.acquire(
      0, 1, RangeMode::write));
  auto result = co_await impl_->open_path(path, flags, mode, directory);
  if (result) {
    auto retained = impl_->retain(result.value().inode);
    if (!retained) {
      auto released = co_await permit.release();
      (void)released;
      co_return Result<OpenedNode>::failure(retained.error());
    }
  }
  auto released = co_await permit.release();
  if (!released && result) {
    auto dropped = co_await impl_->release(result.value());
    (void)dropped;
    co_return Result<OpenedNode>::failure(released.error());
  }
  co_return result;
}

Task<Result<void>> KfsCoroutineCore::close(OpenedNode node) {
  if (node.inode < 0 || node.type == NodeType::unknown) {
    co_return errno_failure<void>(EBADF);
  }
  co_return co_await impl_->release(node);
}

Task<Result<std::size_t>> KfsCoroutineCore::read(
    InodeNumber inode, std::uint64_t offset,
    std::span<std::byte> destination) {
  if (offset > static_cast<std::uint64_t>(
                   std::numeric_limits<std::int64_t>::max()) ||
      destination.size() >
          static_cast<std::uint64_t>(
              std::numeric_limits<std::int64_t>::max()) - offset) {
    co_return errno_failure<std::size_t>(EOVERFLOW);
  }
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  ORCHFS_TRY(range, impl_->range_for(inode));
  co_return co_await detail::with_range_lock(
      *range, offset, range_length(offset, destination.size()),
      RangeMode::read, [&] {
        return impl_->read_unlocked(inode, offset, destination);
      });
}

Task<Result<WriteResult>> KfsCoroutineCore::write(
    InodeNumber inode, std::uint64_t offset,
    std::span<const std::byte> source, bool append) {
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  ORCHFS_TRY(range, impl_->range_for(inode));
  const std::uint64_t range_start = append ? 0 : offset;
  const std::uint64_t length = append ? kWholeFile
                                      : range_length(offset, source.size());
  co_return co_await detail::with_range_lock(
      *range, range_start, length, RangeMode::write, [&] {
        return impl_->write_unlocked(inode, offset, source, append);
      });
}

Task<Result<FileStat>> KfsCoroutineCore::stat(std::string path) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::read,
      [&]() -> Task<Result<FileStat>> {
        auto located = co_await impl_->resolve(path);
        if (!located) {
          co_return Result<FileStat>::failure(located.error());
        }
        impl_->publish(located.value().inode);
        co_return Result<FileStat>::success(file_stat(located.value().inode));
      });
}

Task<Result<FileStat>> KfsCoroutineCore::stat(InodeNumber inode) {
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  auto cached = impl_->cached(inode);
  if (cached) {
    co_return cached;
  }
  orchfs_core_inode snapshot{};
  const int error = orchfs_core_snapshot(inode, &snapshot);
  if (error != 0) {
    co_return errno_failure<FileStat>(error);
  }
  impl_->publish(snapshot);
  co_return Result<FileStat>::success(file_stat(snapshot));
}

Task<Result<FileSystemStat>> KfsCoroutineCore::statfs(InodeNumber inode) {
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  orchfs_core_inode snapshot{};
  const int error = orchfs_core_snapshot(inode, &snapshot);
  if (error != 0) {
    co_return errno_failure<FileSystemStat>(error);
  }
  co_return Result<FileSystemStat>::success(FileSystemStat{
      .type = EXT2_SUPER_MAGIC,
      .block_size = kBlockSize,
      .blocks = (std::uint64_t{1} << 40U) / kBlockSize,
      .blocks_free = (std::uint64_t{1} << 39U) / kBlockSize,
      .blocks_available = (std::uint64_t{1} << 39U) / kBlockSize,
      .files = std::uint64_t{1} << 21U,
      .files_free = std::uint64_t{1} << 20U,
      .name_length = ORCHFS_CORE_DIRENT_NAME_MAX,
      .fragment_size = kPageSize,
      .flags = 0,
  });
}

Task<Result<std::uint64_t>> KfsCoroutineCore::seek(
    InodeNumber inode, std::uint64_t current_offset,
    std::int64_t offset, int whence) {
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  std::uint64_t base = 0;
  if (whence == SEEK_CUR) {
    base = current_offset;
  } else if (whence == SEEK_END) {
    orchfs_core_inode snapshot{};
    const int error = orchfs_core_snapshot(inode, &snapshot);
    if (error != 0) {
      co_return errno_failure<std::uint64_t>(error);
    }
    if (snapshot.size < 0) {
      co_return errno_failure<std::uint64_t>(EIO);
    }
    base = static_cast<std::uint64_t>(snapshot.size);
  } else if (whence != SEEK_SET) {
    co_return errno_failure<std::uint64_t>(EINVAL);
  }
  std::uint64_t value = 0;
  if (offset < 0) {
    const auto magnitude = static_cast<std::uint64_t>(-(offset + 1)) + 1;
    if (magnitude > base) {
      co_return errno_failure<std::uint64_t>(EINVAL);
    }
    value = base - magnitude;
  } else {
    if (static_cast<std::uint64_t>(offset) >
        static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max()) - base) {
      co_return errno_failure<std::uint64_t>(EOVERFLOW);
    }
    value = base + static_cast<std::uint64_t>(offset);
  }
  co_return Result<std::uint64_t>::success(value);
}

Task<Result<void>> KfsCoroutineCore::truncate(std::string path,
                                               std::uint64_t size) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  ORCHFS_TRY(inode, co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::read,
      [&]() -> Task<Result<InodeNumber>> {
        auto located = co_await impl_->resolve(path);
        if (!located) {
          co_return Result<InodeNumber>::failure(located.error());
        }
        co_return Result<InodeNumber>::success(located.value().inode.inode);
      }));
  co_return co_await truncate(inode, size);
}

Task<Result<void>> KfsCoroutineCore::truncate(InodeNumber inode,
                                               std::uint64_t size) {
  if (size > kMaximumFileSize) {
    co_return errno_failure<void>(EFBIG);
  }
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  ORCHFS_TRY(range, impl_->range_for(inode));
  co_return co_await detail::with_range_lock(
      *range, 0, kWholeFile, RangeMode::write,
      [&, this]() -> Task<Result<void>> {
        orchfs_core_inode snapshot{};
        int error = orchfs_core_snapshot(inode, &snapshot);
        Result<void> result = error == 0 ? Result<void>::success()
                                         : errno_failure<void>(error);
        if (result && snapshot.size < 0) {
          result = errno_failure<void>(EIO);
        }
        if (result && snapshot.type != ORCHFS_CORE_REGULAR) {
          result = errno_failure<void>(EISDIR);
        }
        const std::uint64_t old_extent_size = snapshot.size > 0
            ? static_cast<std::uint64_t>(snapshot.size)
            : 0;
        if (result && size > static_cast<std::uint64_t>(snapshot.size)) {
          static const std::array<std::byte, kYieldBytes> zeros{};
          std::uint64_t cursor = static_cast<std::uint64_t>(snapshot.size);
          while (result && cursor < size) {
            const auto chunk = static_cast<std::size_t>(
                std::min<std::uint64_t>(zeros.size(), size - cursor));
            auto written = co_await impl_->write_unlocked(
                inode, cursor,
                std::span<const std::byte>(zeros.data(), chunk), false);
            if (!written) {
              result = Result<void>::failure(written.error());
            } else if (written.value().bytes != chunk) {
              result = errno_failure<void>(EIO);
            } else {
              cursor += chunk;
            }
          }
        } else if (result) {
          auto transaction_result = LogTransaction::create();
          if (!transaction_result) {
            result = Result<void>::failure(transaction_result.error());
          } else {
            auto transaction = std::move(transaction_result).value();
            error = transaction->invoke(
                [&] { return orchfs_core_set_size(inode, size); });
            if (error != 0) {
              result = errno_failure<void>(error);
            } else {
              result = co_await impl_->journal_.commit(transaction->release());
            }
            if (result && orchfs_core_snapshot(inode, &snapshot) == 0) {
              impl_->publish(snapshot);
              impl_->publish_extent_update(
                  inode, old_extent_size,
                  static_cast<std::uint64_t>(snapshot.size), 0, {});
            } else if (result) {
              impl_->erase_extents(inode);
            }
          }
        }
        co_return result;
      });
}

Task<Result<void>> KfsCoroutineCore::sync(InodeNumber inode) {
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  ORCHFS_TRY(range, impl_->range_for(inode));
  co_return co_await detail::with_range_lock(
      *range, 0, kWholeFile, RangeMode::write,
      [&]() -> Task<Result<void>> {
        const int metadata_error = orchfs_core_sync_inode(inode);
        co_return metadata_error == 0
            ? co_await impl_->device_.flush()
            : errno_failure<void>(metadata_error);
      });
}

Task<Result<void>> KfsCoroutineCore::make_directory(
    std::string path, std::uint32_t mode) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::write,
      [&, this]() -> Task<Result<void>> {
        auto parent = co_await impl_->resolve_parent(path);
        Result<void> result = parent
            ? Result<void>::success()
            : Result<void>::failure(parent.error());
        if (result) {
          auto created = co_await impl_->create_node(
              parent.value().parent, std::move(parent.value().name),
              ORCHFS_CORE_DIRECTORY, mode);
          result = created ? Result<void>::success()
                           : Result<void>::failure(created.error());
        }
        co_return result;
      });
}

Task<Result<void>> KfsCoroutineCore::remove_directory(std::string path) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::write, [&] {
        return impl_->remove_path(path, ORCHFS_CORE_DIRECTORY);
      });
}

Task<Result<void>> KfsCoroutineCore::unlink(std::string path) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::write, [&] {
        return impl_->remove_path(path, ORCHFS_CORE_REGULAR);
      });
}

Task<Result<void>> KfsCoroutineCore::rename(std::string old_path,
                                             std::string new_path) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return co_await detail::with_range_lock(
      impl_->namespace_gate_, 0, 1, RangeMode::write, [&] {
        return impl_->rename_path(old_path, new_path);
      });
}

Task<Result<OpenedNode>> KfsCoroutineCore::open_directory(std::string path) {
  ORCHFS_TRY(opened,
             co_await open(std::move(path), O_RDONLY | O_DIRECTORY, 0));
  if (opened.type != NodeType::directory) {
    co_return errno_failure<OpenedNode>(ENOTDIR);
  }
  co_return Result<OpenedNode>::success(std::move(opened));
}

Task<Result<OpenedNode>> KfsCoroutineCore::open_directory(InodeNumber inode) {
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  orchfs_core_inode snapshot{};
  const int error = orchfs_core_snapshot(inode, &snapshot);
  if (error != 0) {
    co_return errno_failure<OpenedNode>(error);
  }
  if (snapshot.type != ORCHFS_CORE_DIRECTORY) {
    co_return errno_failure<OpenedNode>(ENOTDIR);
  }
  impl_->publish(snapshot);
  ORCHFS_TRYV(impl_->retain(inode));
  co_return Result<OpenedNode>::success(opened_node(snapshot));
}

Task<Result<DirectoryReadResult>> KfsCoroutineCore::read_directory(
    InodeNumber inode, std::uint64_t cursor, std::span<DirEntry> entries) {
  if (cursor % ORCHFS_CORE_DIRENT_SIZE != 0) {
    co_return errno_failure<DirectoryReadResult>(EINVAL);
  }
  ORCHFS_TRYV(co_await impl_->schedule_inode_owner(inode));
  ORCHFS_TRY(range, impl_->range_for(inode));
  co_return co_await detail::with_range_lock(
      *range, 0, kWholeFile, RangeMode::read,
      [&, this]() -> Task<Result<DirectoryReadResult>> {
        orchfs_core_inode snapshot{};
        int error = orchfs_core_snapshot(inode, &snapshot);
        Result<DirectoryReadResult> result =
            error == 0
                ? Result<DirectoryReadResult>::success(
                      DirectoryReadResult{.count = 0, .next_cursor = cursor})
                : errno_failure<DirectoryReadResult>(error);
        if (result && snapshot.type != ORCHFS_CORE_DIRECTORY) {
          result = errno_failure<DirectoryReadResult>(ENOTDIR);
        }

        std::size_t output_count = 0;
        std::uint64_t next_cursor = cursor;
        while (result && output_count < entries.size() &&
               next_cursor < static_cast<std::uint64_t>(snapshot.size)) {
          const std::uint64_t available =
              static_cast<std::uint64_t>(snapshot.size) - next_cursor;
          const std::size_t window = static_cast<std::size_t>(
              std::min<std::uint64_t>(
                  kDirectoryWindowEntries * ORCHFS_CORE_DIRENT_SIZE,
                  available));
          const std::size_t aligned_window =
              window - window % ORCHFS_CORE_DIRENT_SIZE;
          if (aligned_window == 0) {
            result = errno_failure<DirectoryReadResult>(EIO);
            break;
          }
          std::vector<std::byte> buffer;
          try {
            buffer.resize(aligned_window);
          } catch (const std::bad_alloc&) {
            result = errno_failure<DirectoryReadResult>(ENOMEM);
            break;
          }
          auto read = co_await impl_->read_unlocked(
              inode, next_cursor, buffer);
          if (!read) {
            result = Result<DirectoryReadResult>::failure(read.error());
            break;
          }
          if (read.value() != buffer.size()) {
            result = errno_failure<DirectoryReadResult>(EIO);
            break;
          }
          for (std::size_t offset = 0;
               offset < buffer.size() && output_count < entries.size();
               offset += ORCHFS_CORE_DIRENT_SIZE) {
            orchfs_core_dirent entry{};
            std::memcpy(&entry, buffer.data() + offset, sizeof(entry));
            next_cursor += ORCHFS_CORE_DIRENT_SIZE;
            if (entry.type == 0) {
              continue;
            }
            if (entry.name_length > ORCHFS_CORE_DIRENT_NAME_MAX ||
                std::memchr(entry.name, '\0', entry.name_length) != nullptr) {
              result = errno_failure<DirectoryReadResult>(EIO);
              break;
            }
            entries[output_count++] = DirEntry{
                .inode = static_cast<std::uint64_t>(entry.inode),
                .offset = static_cast<std::int64_t>(next_cursor),
                .type = entry.type,
                .name = std::string(entry.name, entry.name_length),
            };
          }
        }
        if (result) {
          result = Result<DirectoryReadResult>::success(DirectoryReadResult{
              .count = output_count,
              .next_cursor = next_cursor,
          });
        }
        co_return result;
      });
}

Task<Result<bool>> KfsCoroutineCore::migrate(std::size_t max_operations) {
  for (std::size_t migrated = 0; migrated < max_operations; ++migrated) {
    ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
    ORCHFS_TRY(namespace_permit, co_await impl_->namespace_gate_.acquire(
        0, 1, RangeMode::read));

    orchfs_migration_plan* plan = nullptr;
    int error = orchfs_prepare_migration(&plan);
    auto namespace_released = co_await namespace_permit.release();
    if (!namespace_released) {
      if (plan != nullptr) {
        orchfs_finish_migration(plan, ECANCELED);
      }
      co_return Result<bool>::failure(namespace_released.error());
    }
    if (error == EAGAIN) {
      co_return Result<bool>::success(false);
    }
    if (error != 0) {
      co_return errno_failure<bool>(error);
    }

    const InodeNumber inode = orchfs_migration_inode(plan);
    const std::uint64_t migration_file_offset =
        orchfs_migration_file_offset(plan);
    const std::uint64_t migration_length = orchfs_migration_length(plan);
    auto inode_scheduled = co_await impl_->schedule_inode_owner(inode);
    if (!inode_scheduled) {
      orchfs_finish_migration(plan, ECANCELED);
      co_return Result<bool>::failure(inode_scheduled.error());
    }
    auto range = impl_->range_for(inode);
    if (!range) {
      orchfs_finish_migration(plan, ECANCELED);
      co_return Result<bool>::failure(range.error());
    }
    auto range_acquired = co_await range.value()->acquire(
        migration_file_offset, migration_length, RangeMode::write);
    if (!range_acquired) {
      orchfs_finish_migration(plan, ECANCELED);
      co_return Result<bool>::failure(range_acquired.error());
    }
    auto range_permit = std::move(range_acquired).value();

    error = orchfs_prepare_migration_io(plan);
    if (error == 0) {
      const auto data = std::span(
          static_cast<const std::byte*>(orchfs_migration_data(plan)),
          static_cast<std::size_t>(orchfs_migration_length(plan)));
      auto written = co_await impl_->device_.write(
          orchfs_migration_device_offset(plan), data);
      if (!written) {
        error = written.error().value() > 0 ? written.error().value() : EIO;
      } else if (written.value() != data.size()) {
        error = EIO;
      }
    }
    if (error == 0) {
      auto transaction_result = LogTransaction::create();
      if (!transaction_result) {
        error = transaction_result.error().value();
        orchfs_finish_migration(plan, error);
        plan = nullptr;
      } else {
        auto transaction = std::move(transaction_result).value();
        error = orchfs_log_transaction_record_allocation(
            transaction->get(), kSsdBlockExtent,
            orchfs_migration_new_ssd_block(plan));
        if (error == 0) {
          error = transaction->invoke(
              [&] { return orchfs_finish_migration(plan, 0); });
          plan = nullptr;
          if (error == 0) {
            auto committed =
                co_await impl_->journal_.commit(transaction->release());
            if (!committed) {
              error = committed.error().value() > 0
                  ? committed.error().value() : EIO;
            }
          }
        } else {
          orchfs_finish_migration(plan, error);
          plan = nullptr;
        }
      }
    } else {
      orchfs_finish_migration(plan, error);
      plan = nullptr;
    }

    if (error == 0) {
      impl_->refresh_extent_range(inode, migration_file_offset,
                                  migration_length);
    }

    auto range_released = co_await range_permit.release();
    if (!range_released) {
      co_return Result<bool>::failure(range_released.error());
    }
    if (error != 0 && error != ESTALE) {
      co_return errno_failure<bool>(error);
    }
  }
  ORCHFS_TRYV(co_await impl_->schedule_namespace_owner());
  co_return Result<bool>::success(orchfs_migration_has_pending() != 0);
}

Task<Result<void>> KfsCoroutineCore::close_directory(OpenedNode node) {
  if (node.type != NodeType::directory) {
    co_return errno_failure<void>(EBADF);
  }
  co_return co_await impl_->release(node);
}

}  // namespace orchfs::async
