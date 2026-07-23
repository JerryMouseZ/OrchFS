#include "orchfs/async/kfs_coroutine_core.hpp"
#include "orchfs/async/runtime.hpp"
#include "orchfs/repro_trace.h"

extern "C" {
#include "../KernelFS/async_device.h"
#include "../LibFS/kfs_core_api.h"
#include "../LibFS/lib_log.h"
#include "../LibFS/migrate.h"
}

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <new>
#include <span>
#include <string_view>
#include <sys/stat.h>
#include <utility>
#include <vector>

namespace {

constexpr std::size_t kPageSize = 4096;
constexpr std::size_t kBlockSize = 32 * 1024;
constexpr std::size_t kPageCount = kBlockSize / kPageSize;
constexpr std::size_t kPmemSize = 16 * 1024 * 1024;
constexpr std::size_t kMetadataBase = 8 * 1024 * 1024;
constexpr std::int64_t kInode = 7;

struct BlockState {
  std::int64_t type{ORCHFS_CORE_EMPTY_BLOCK};
  std::int64_t ssd_offset{-1};
  std::array<std::int64_t, kPageCount> nvm{};
  std::array<std::int64_t, kPageCount> metadata{};

  BlockState() {
    nvm.fill(-1);
    metadata.fill(-1);
  }
};

struct FileState {
  std::int64_t size{};
  std::vector<BlockState> blocks;
};

FileState file_state;
std::vector<std::byte> pmem(kPmemSize);
std::vector<std::byte> device(4 * 1024 * 1024);
std::atomic<int> device_error{0};
std::atomic<int> device_durability{ORCHFS_DEVICE_DURABILITY_COMPLETION};
std::atomic<std::size_t> device_flush_count{0};
std::atomic<bool> fail_allocation{false};
std::atomic<bool> migration_candidate{false};
std::atomic<std::size_t> block_query_count{0};
std::atomic<std::size_t> strata_ensure_count{0};
thread_local orchfs_log_transaction* bound_transaction{};

struct PendingPmem {
  std::int64_t offset{};
  std::vector<std::byte> bytes;
};

[[noreturn]] void fail(std::string_view message) {
  std::fprintf(stderr, "KFS coroutine core test failure: %.*s\n",
               static_cast<int>(message.size()), message.data());
  std::abort();
}

void require(bool condition, std::string_view message) {
  if (!condition) {
    fail(message);
  }
}

std::byte pattern(std::size_t position) {
  return static_cast<std::byte>((position * 131 + 17) % 251);
}

BlockState virtual_block(std::size_t block) {
  BlockState state;
  state.type = ORCHFS_CORE_VIRTUAL_BLOCK;
  for (std::size_t page = 0; page < kPageCount; ++page) {
    state.nvm[page] = static_cast<std::int64_t>(
        block * kBlockSize + page * kPageSize);
  }
  return state;
}

BlockState ssd_block(std::size_t block) {
  BlockState state;
  state.type = ORCHFS_CORE_SSD_BLOCK;
  state.ssd_offset = static_cast<std::int64_t>(block * kBlockSize);
  return state;
}

void reset_ssd(std::size_t size) {
  file_state = {};
  file_state.size = static_cast<std::int64_t>(size);
  file_state.blocks.resize((size + kBlockSize - 1) / kBlockSize);
  std::fill(pmem.begin(), pmem.end(), std::byte{});
  std::fill(device.begin(), device.end(), std::byte{});
  for (std::size_t block = 0; block < file_state.blocks.size(); ++block) {
    file_state.blocks[block] = ssd_block(block);
  }
  for (std::size_t i = 0; i < size; ++i) {
    device[i] = pattern(i);
  }
  device_error.store(0, std::memory_order_release);
  device_durability.store(ORCHFS_DEVICE_DURABILITY_COMPLETION,
                          std::memory_order_release);
  device_flush_count.store(0, std::memory_order_release);
  fail_allocation.store(false, std::memory_order_release);
  strata_ensure_count.store(0, std::memory_order_release);
}

void reset_virtual(std::size_t size) {
  file_state = {};
  file_state.size = static_cast<std::int64_t>(size);
  file_state.blocks.resize((size + kBlockSize - 1) / kBlockSize);
  std::fill(pmem.begin(), pmem.end(), std::byte{});
  std::fill(device.begin(), device.end(), std::byte{});
  for (std::size_t block = 0; block < file_state.blocks.size(); ++block) {
    file_state.blocks[block] = virtual_block(block);
  }
  device_error.store(0, std::memory_order_release);
  device_durability.store(ORCHFS_DEVICE_DURABILITY_COMPLETION,
                          std::memory_order_release);
  device_flush_count.store(0, std::memory_order_release);
  fail_allocation.store(false, std::memory_order_release);
  strata_ensure_count.store(0, std::memory_order_release);
}

void copy_mapping(std::size_t block, orchfs_core_block* output) {
  const auto& state = file_state.blocks[block];
  output->type = state.type;
  output->ssd_device_offset = state.ssd_offset;
  output->file_block = static_cast<std::int64_t>(block);
  std::copy(state.nvm.begin(), state.nvm.end(), output->nvm_page_offset);
  std::copy(state.metadata.begin(), state.metadata.end(),
            output->buffer_metadata_offset);
}

template <typename T>
orchfs::async::Result<T> run(
    orchfs::async::Runtime& runtime,
    orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  require(static_cast<bool>(submitted), "Runtime submit failed");
  auto joined = std::move(submitted).value().join();
  require(static_cast<bool>(joined), "Runtime join handle failed");
  return std::move(joined).value();
}

std::vector<std::byte> read_file(
    orchfs::async::Runtime& runtime,
    const std::shared_ptr<orchfs::async::KfsCoroutineCore>& core,
    std::size_t length) {
  std::vector<std::byte> bytes(length);
  auto read = run(runtime, core->read(kInode, 0, bytes));
  require(read && read.value() == length, "core read length mismatch");
  return bytes;
}

orchfs::async::Task<orchfs::async::Result<std::size_t>> heartbeat(
    std::atomic<bool>& stop) {
  std::size_t pulses = 0;
  while (!stop.load(std::memory_order_acquire)) {
    ++pulses;
    auto yielded = co_await orchfs::async::Runtime::yield();
    if (!yielded) {
      co_return orchfs::async::Result<std::size_t>::failure(yielded.error());
    }
  }
  co_return orchfs::async::Result<std::size_t>::success(pulses);
}

}  // namespace

struct orchfs_log_transaction {
  std::int64_t original_size{};
  std::vector<BlockState> original_blocks;
  std::vector<PendingPmem> pmem_writes;
};

struct orchfs_migration_plan {
  std::array<std::byte, kBlockSize> data{};
  std::uint64_t device_offset{2 * kBlockSize};
  std::int64_t new_ssd_block{2};
  bool prepared{};
};

extern "C" int orchfs_log_transaction_create(
    orchfs_log_transaction** output) {
  if (output == nullptr) {
    return EINVAL;
  }
  try {
    auto transaction = std::make_unique<orchfs_log_transaction>();
    transaction->original_size = file_state.size;
    transaction->original_blocks = file_state.blocks;
    *output = transaction.release();
    return 0;
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  }
}

extern "C" void orchfs_log_transaction_bind(
    orchfs_log_transaction* transaction) {
  require(transaction != nullptr && bound_transaction == nullptr,
          "invalid transaction bind");
  bound_transaction = transaction;
}

extern "C" void orchfs_log_transaction_unbind(
    orchfs_log_transaction* transaction) {
  require(transaction != nullptr && bound_transaction == transaction,
          "invalid transaction unbind");
  bound_transaction = nullptr;
}

extern "C" int orchfs_log_transaction_add_pmem(
    orchfs_log_transaction* transaction, std::int64_t offset,
    const void* source, std::int32_t length) {
  if (transaction == nullptr || offset < 0 || length < 0 ||
      (length != 0 && source == nullptr) ||
      static_cast<std::uint64_t>(offset) + static_cast<std::uint32_t>(length) >
          pmem.size()) {
    return EINVAL;
  }
  try {
    PendingPmem write;
    write.offset = offset;
    const auto* begin = static_cast<const std::byte*>(source);
    write.bytes.assign(begin, begin + length);
    transaction->pmem_writes.push_back(std::move(write));
    return 0;
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  }
}

extern "C" int orchfs_log_transaction_commit(
    orchfs_log_transaction* transaction) {
  if (transaction == nullptr) {
    return EINVAL;
  }
  for (const auto& write : transaction->pmem_writes) {
    std::copy(write.bytes.begin(), write.bytes.end(),
              pmem.begin() + write.offset);
  }
  delete transaction;
  return 0;
}

extern "C" int orchfs_log_transaction_is_empty(
    const orchfs_log_transaction* transaction) {
  if (transaction == nullptr || !transaction->pmem_writes.empty() ||
      transaction->original_size != file_state.size ||
      transaction->original_blocks.size() != file_state.blocks.size()) {
    return 0;
  }
  for (std::size_t index = 0; index < file_state.blocks.size(); ++index) {
    const auto& before = transaction->original_blocks[index];
    const auto& after = file_state.blocks[index];
    if (before.type != after.type || before.ssd_offset != after.ssd_offset ||
        before.nvm != after.nvm || before.metadata != after.metadata) {
      return 0;
    }
  }
  return 1;
}

extern "C" int orchfs_log_transaction_commit_group(
    orchfs_log_transaction** transactions, std::size_t count, int* errors) {
  if (count != 0 && (transactions == nullptr || errors == nullptr)) {
    return EINVAL;
  }
  for (std::size_t index = 0; index < count; ++index) {
    if (transactions[index] == nullptr) {
      return EINVAL;
    }
  }
  for (std::size_t index = 0; index < count; ++index) {
    errors[index] = orchfs_log_transaction_commit(transactions[index]);
  }
  return 0;
}

extern "C" void orchfs_log_transaction_abort(
    orchfs_log_transaction* transaction) {
  if (transaction == nullptr) {
    return;
  }
  if (bound_transaction == transaction) {
    bound_transaction = nullptr;
  }
  file_state.size = transaction->original_size;
  file_state.blocks = std::move(transaction->original_blocks);
  delete transaction;
}

extern "C" int orchfs_log_transaction_record_allocation(
    orchfs_log_transaction*, int, std::int64_t) {
  return 0;
}

extern "C" int orchfs_core_snapshot(
    std::int64_t inode, orchfs_core_inode* output) {
  if (inode != kInode || output == nullptr) {
    return ENOENT;
  }
  *output = {};
  output->inode = kInode;
  output->size = file_state.size;
  output->index_root = 1;
  output->link_count = 1;
  output->mode = S_IFREG | 0644;
  output->type = ORCHFS_CORE_REGULAR;
  return 0;
}

extern "C" int orchfs_core_query_block(
    std::int64_t inode, std::int64_t block, orchfs_core_block* output) {
  if (inode != kInode || block < 0 || output == nullptr ||
      static_cast<std::size_t>(block) >= file_state.blocks.size()) {
    return ENODATA;
  }
  block_query_count.fetch_add(1, std::memory_order_relaxed);
  copy_mapping(static_cast<std::size_t>(block), output);
  return 0;
}

extern "C" int orchfs_core_prepare_write_blocks(
    std::int64_t inode, std::int64_t first, std::int64_t last,
    std::int64_t, orchfs_core_block* output) {
  if (inode != kInode || first < 0 || last < first || output == nullptr) {
    return EINVAL;
  }
  if (fail_allocation.load(std::memory_order_acquire)) {
    return ENOSPC;
  }
  try {
    if (file_state.blocks.size() <= static_cast<std::size_t>(last)) {
      const auto old_size = file_state.blocks.size();
      file_state.blocks.resize(static_cast<std::size_t>(last) + 1);
      for (std::size_t block = old_size; block < file_state.blocks.size();
           ++block) {
        file_state.blocks[block] = virtual_block(block);
      }
    }
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  }
  for (std::int64_t block = first; block <= last; ++block) {
    copy_mapping(static_cast<std::size_t>(block), output + block - first);
  }
  return 0;
}

extern "C" int orchfs_core_ensure_strata(
    std::int64_t inode, std::int64_t block, std::int64_t start,
    std::int64_t length, orchfs_core_block* output) {
  if (inode != kInode || block < 0 || output == nullptr ||
      start < 0 || length <= 0 || start >= kBlockSize ||
      length > static_cast<std::int64_t>(kBlockSize) - start ||
      static_cast<std::size_t>(block) >= file_state.blocks.size()) {
    return EINVAL;
  }
  strata_ensure_count.fetch_add(1, std::memory_order_relaxed);
  auto& state = file_state.blocks[static_cast<std::size_t>(block)];
  if (state.type != ORCHFS_CORE_SSD_BLOCK &&
      state.type != ORCHFS_CORE_STRATA_BLOCK) {
    return EINVAL;
  }
  state.type = ORCHFS_CORE_STRATA_BLOCK;
  const std::size_t first_page = static_cast<std::size_t>(start) / kPageSize;
  const std::size_t last_page =
      static_cast<std::size_t>(start + length - 1) / kPageSize;
  for (std::size_t page = first_page; page <= last_page; ++page) {
    if (state.nvm[page] >= 0) {
      continue;
    }
    state.nvm[page] = block * static_cast<std::int64_t>(kBlockSize) +
                      static_cast<std::int64_t>(page * kPageSize);
    state.metadata[page] = static_cast<std::int64_t>(
        kMetadataBase + static_cast<std::size_t>(block) * kPageCount * 128 +
        page * 128);
  }
  copy_mapping(static_cast<std::size_t>(block), output);
  return 0;
}

extern "C" int orchfs_core_read_pmem(
    std::int64_t offset, void* destination, std::size_t length) {
  if (offset < 0 || (length != 0 && destination == nullptr) ||
      static_cast<std::uint64_t>(offset) + length > pmem.size()) {
    return EINVAL;
  }
  std::memcpy(destination, pmem.data() + offset, length);
  return 0;
}

extern "C" int orchfs_core_write_pmem(
    std::int64_t offset, const void* source, std::size_t length) {
  if (offset < 0 || (length != 0 && source == nullptr) ||
      static_cast<std::uint64_t>(offset) + length > pmem.size()) {
    return EINVAL;
  }
  std::memcpy(pmem.data() + offset, source, length);
  return 0;
}

extern "C" int orchfs_core_set_size(std::int64_t inode,
                                      std::uint64_t size) {
  if (inode != kInode || size > INT64_MAX) {
    return EINVAL;
  }
  file_state.size = static_cast<std::int64_t>(size);
  return 0;
}

extern "C" int orchfs_core_sync_inode(std::int64_t inode) {
  return inode == kInode ? 0 : ENOENT;
}

extern "C" int orchfs_core_persist_pmem() { return 0; }

extern "C" std::int64_t orchfs_core_root_inode(void) { return kInode; }

extern "C" int orchfs_core_create_inode(
    std::int32_t, std::uint32_t, orchfs_core_inode*) {
  return ENOTSUP;
}

extern "C" int orchfs_core_delete_inode(std::int64_t) { return ENOTSUP; }
extern "C" int orchfs_core_set_orphan(std::int64_t, int) { return ENOTSUP; }

extern "C" int submit_read_data_from_devs(
    void* destination, std::int64_t length, std::int64_t offset,
    orchfs_device_completion_fn completion, void* context) {
  const int error = device_error.load(std::memory_order_acquire);
  if (error != 0) {
    return error;
  }
  if (offset < 0 || length < 0 ||
      static_cast<std::uint64_t>(offset) +
              static_cast<std::uint64_t>(length) >
          device.size()) {
    return EINVAL;
  }
  std::memcpy(destination, device.data() + offset,
              static_cast<std::size_t>(length));
  completion(context, 0, static_cast<std::size_t>(length));
  return 0;
}

extern "C" int submit_write_data_to_devs(
    const void* source, std::int64_t length, std::int64_t offset,
    orchfs_device_completion_fn completion, void* context) {
  const int error = device_error.load(std::memory_order_acquire);
  if (error != 0) {
    return error;
  }
  if (offset < 0 || length < 0 ||
      static_cast<std::uint64_t>(offset) +
              static_cast<std::uint64_t>(length) >
          device.size()) {
    return EINVAL;
  }
  std::memcpy(device.data() + offset, source,
              static_cast<std::size_t>(length));
  completion(context, 0, static_cast<std::size_t>(length));
  return 0;
}

extern "C" int submit_device_sync(
    orchfs_device_completion_fn completion, void* context) {
  const int error = device_error.load(std::memory_order_acquire);
  if (error != 0) {
    return error;
  }
  device_flush_count.fetch_add(1, std::memory_order_relaxed);
  completion(context, 0, 0);
  return 0;
}

extern "C" int orchfs_device_effective_write_durability() {
  return device_durability.load(std::memory_order_acquire);
}

extern "C" int orchfs_prepare_migration(orchfs_migration_plan** output) {
  if (output == nullptr) {
    return EINVAL;
  }
  *output = nullptr;
  if (!migration_candidate.exchange(false, std::memory_order_acq_rel)) {
    return EAGAIN;
  }
  *output = new (std::nothrow) orchfs_migration_plan;
  if (*output == nullptr) {
    migration_candidate.store(true, std::memory_order_release);
    return ENOMEM;
  }
  return 0;
}
extern "C" std::int64_t orchfs_migration_inode(
    const orchfs_migration_plan*) { return kInode; }
extern "C" std::uint64_t orchfs_migration_file_offset(
    const orchfs_migration_plan*) { return 0; }
extern "C" int orchfs_prepare_migration_io(orchfs_migration_plan* plan) {
  if (plan == nullptr || file_state.blocks.empty() ||
      file_state.blocks.front().type != ORCHFS_CORE_VIRTUAL_BLOCK) {
    return ESTALE;
  }
  std::copy_n(pmem.begin(), kBlockSize, plan->data.begin());
  plan->prepared = true;
  return 0;
}
extern "C" const void* orchfs_migration_data(
    const orchfs_migration_plan* plan) {
  return plan != nullptr ? plan->data.data() : nullptr;
}
extern "C" std::uint64_t orchfs_migration_length(
    const orchfs_migration_plan* plan) {
  return plan != nullptr ? kBlockSize : 0;
}
extern "C" std::uint64_t orchfs_migration_device_offset(
    const orchfs_migration_plan* plan) {
  return plan != nullptr ? plan->device_offset : 0;
}
extern "C" std::int64_t orchfs_migration_new_ssd_block(
    const orchfs_migration_plan* plan) {
  return plan != nullptr ? plan->new_ssd_block : -1;
}
extern "C" int orchfs_finish_migration(orchfs_migration_plan* plan,
                                         int error) {
  if (plan == nullptr) {
    return EINVAL;
  }
  if (error == 0 && plan->prepared) {
    file_state.blocks.front() = ssd_block(2);
  } else if (error != ESTALE) {
    migration_candidate.store(true, std::memory_order_release);
  }
  delete plan;
  return error;
}
extern "C" int orchfs_migration_has_pending(void) {
  return migration_candidate.load(std::memory_order_acquire) ? 1 : 0;
}

int main() {
  orchfs::async::RuntimeOptions options;
  options.worker_count = 2;
  auto created = orchfs::async::Runtime::create(std::move(options));
  require(static_cast<bool>(created), "Runtime create failed");
  auto runtime = std::move(created).value();
  auto core_result = orchfs::async::KfsCoroutineCore::create(*runtime);
  require(static_cast<bool>(core_result), "KFS core create failed");
  auto core = std::move(core_result).value();

  constexpr std::array<std::size_t, 10> boundaries{
      1, 511, 512, 513, 4095, 4096, 4097, 32767, 32768, 32769};
  constexpr std::size_t file_size = 4 * kBlockSize;
  for (const auto boundary : boundaries) {
    reset_ssd(file_size);
    std::vector<std::byte> expected(file_size);
    std::copy_n(device.begin(), file_size, expected.begin());
    std::vector<std::byte> source(boundary);
    for (std::size_t i = 0; i < source.size(); ++i) {
      source[i] = static_cast<std::byte>((i * 29 + boundary) % 253);
    }
    auto written = run(*runtime, core->write(kInode, boundary, source, false));
    require(written && written.value().bytes == boundary &&
                written.value().offset == boundary,
            "boundary write failed");
    std::copy(source.begin(), source.end(), expected.begin() + boundary);
    require(read_file(*runtime, core, file_size) == expected,
            "unaligned RMW changed protected bytes");
  }

  reset_ssd(file_size);
  const auto strata_before_large =
      strata_ensure_count.load(std::memory_order_acquire);
  std::vector<std::byte> large_partial(kBlockSize + 1, std::byte{0x63});
  auto large_partial_write = run(
      *runtime, core->write(kInode, 1, large_partial, false));
  require(large_partial_write &&
              large_partial_write.value().bytes == large_partial.size(),
          "large SSD partial overwrite failed");
  require(strata_ensure_count.load(std::memory_order_acquire) ==
              strata_before_large,
          "large SSD partial overwrite allocated STRATA metadata");
  std::vector<std::byte> large_partial_readback(large_partial.size());
  auto large_partial_read = run(
      *runtime, core->read(kInode, 1, large_partial_readback));
  require(large_partial_read && large_partial_readback == large_partial,
          "large SSD partial overwrite readback differed");

  constexpr std::size_t pipelined_file_size = 16 * kBlockSize;
  reset_ssd(pipelined_file_size);
  device_durability.store(ORCHFS_DEVICE_DURABILITY_FLUSH,
                          std::memory_order_release);
  std::vector<std::byte> pipelined_expected(
      device.begin(), device.begin() + pipelined_file_size);
  std::vector<std::byte> pipelined_source(
      10 * kBlockSize + 1, std::byte{0x2d});
  auto pipelined_write = run(
      *runtime, core->write(kInode, 1, pipelined_source, false));
  require(pipelined_write &&
              pipelined_write.value().bytes == pipelined_source.size(),
          "pipelined SSD partial overwrite failed");
  require(device_flush_count.load(std::memory_order_acquire) == 1,
          "pipelined SSD partial overwrite was not covered by one flush");
  std::copy(pipelined_source.begin(), pipelined_source.end(),
            pipelined_expected.begin() + 1);
  require(read_file(*runtime, core, pipelined_file_size) ==
              pipelined_expected,
          "pipelined SSD partial overwrite changed protected bytes");

  for (const std::size_t length : {std::size_t{1024}, kPageSize}) {
    constexpr std::size_t exact_offset = 2854777;
    reset_ssd(4 * 1024 * 1024);
    std::vector<std::byte> source(length);
    for (std::size_t index = 0; index < source.size(); ++index) {
      source[index] = static_cast<std::byte>((index * 47 + length) % 251);
    }
    auto written = run(
        *runtime, core->write(kInode, exact_offset, source, false));
    require(written && written.value().bytes == source.size(),
            "Fig.19 exact-offset write failed");
    std::vector<std::byte> readback(source.size());
    auto read = run(*runtime, core->read(kInode, exact_offset, readback));
    require(read && read.value() == source.size() && readback == source,
            "Fig.19 exact-offset readback mismatch");
  }

  reset_ssd(kBlockSize);
  std::array<std::byte, 1> first_strata_byte{std::byte{0x31}};
  auto first_strata = run(
      *runtime, core->write(kInode, 1, first_strata_byte, false));
  require(first_strata && strata_ensure_count.load(std::memory_order_acquire) == 1,
          "initial SSD-to-STRATA conversion failed");
  std::array<std::byte, 1> later_strata_byte{std::byte{0x72}};
  auto later_strata = run(
      *runtime,
      core->write(kInode, 2 * kPageSize + 7, later_strata_byte, false));
  require(later_strata &&
              strata_ensure_count.load(std::memory_order_acquire) == 2 &&
              file_state.blocks.front().nvm[2] >= 0,
          "existing STRATA block did not allocate a newly touched page");

  reset_ssd(kBlockSize);
  std::vector<std::byte> five_pages(5 * kPageSize, std::byte{0x45});
  const std::vector<std::byte> device_before_five = device;
  auto five_written = run(
      *runtime, core->write(kInode, kPageSize, five_pages, false));
  require(five_written &&
              std::equal(device.begin() + kPageSize,
                         device.begin() + 6 * kPageSize,
                         device_before_five.begin() + kPageSize),
          "five complete STRATA pages bypassed the NVM threshold");
  std::vector<std::byte> five_readback(five_pages.size());
  auto five_read = run(
      *runtime, core->read(kInode, kPageSize, five_readback));
  require(five_read && five_readback == five_pages,
          "five-page NVM STRATA write was not readable");

  reset_ssd(kBlockSize);
  std::vector<std::byte> six_pages(6 * kPageSize, std::byte{0x6a});
  auto six_written = run(
      *runtime, core->write(kInode, kPageSize, six_pages, false));
  require(six_written &&
              std::equal(six_pages.begin(), six_pages.end(),
                         device.begin() + kPageSize),
          "six complete STRATA pages did not use the SSD threshold");

  reset_virtual(16);
  std::array<std::byte, 3> beyond{std::byte{'x'}, std::byte{'y'},
                                  std::byte{'z'}};
  auto sparse = run(*runtime, core->write(kInode, 17, beyond, false));
  require(sparse && sparse.value().bytes == 0 && file_state.size == 16,
          "pwrite beyond EOF created a sparse hole");
  auto at_eof = run(*runtime, core->write(kInode, 16, beyond, false));
  require(at_eof && at_eof.value().bytes == beyond.size() &&
              file_state.size == 19,
          "write at EOF did not extend the file");

  reset_virtual(1);
  pmem[0] = std::byte{'A'};
  constexpr std::size_t append_count = 64;
  constexpr std::size_t record_size = 17;
  std::array<std::array<std::byte, record_size>, append_count> records{};
  std::vector<orchfs::async::JoinHandle<
      orchfs::async::Result<orchfs::async::WriteResult>>> appends;
  appends.reserve(append_count);
  for (std::size_t record = 0; record < append_count; ++record) {
    std::fill(records[record].begin(), records[record].end(),
              static_cast<std::byte>(record + 1));
    auto submitted = runtime->submit(
        core->write(kInode, 0, records[record], true));
    require(static_cast<bool>(submitted), "append submit failed");
    appends.push_back(std::move(submitted).value());
  }
  std::vector<std::uint64_t> offsets;
  offsets.reserve(append_count);
  for (auto& append : appends) {
    auto joined = std::move(append).join();
    require(static_cast<bool>(joined) && joined.value(),
            "concurrent append failed");
    offsets.push_back(joined.value().value().offset);
  }
  std::sort(offsets.begin(), offsets.end());
  for (std::size_t i = 0; i < offsets.size(); ++i) {
    require(offsets[i] == 1 + i * record_size,
            "append ranges overlapped or left a gap");
  }
  auto appended = read_file(*runtime, core,
                            1 + append_count * record_size);
  require(appended.front() == std::byte{'A'}, "append damaged prefix");
  for (std::size_t slot = 0; slot < append_count; ++slot) {
    const auto value = appended[1 + slot * record_size];
    for (std::size_t byte = 0; byte < record_size; ++byte) {
      require(appended[1 + slot * record_size + byte] == value,
              "append record was interleaved");
    }
  }

  reset_virtual(0);
  constexpr std::size_t grown_size = 1024 * 1024 + 513;
  auto grown = run(*runtime, core->truncate(kInode, grown_size));
  require(grown && file_state.size == static_cast<std::int64_t>(grown_size),
          "truncate grow failed");
  const auto grown_bytes = read_file(*runtime, core, grown_size);
  require(std::all_of(grown_bytes.begin(), grown_bytes.end(),
                      [](std::byte value) { return value == std::byte{}; }),
          "truncate grow did not zero-fill");
  auto shrunk = run(*runtime, core->truncate(kInode, 513));
  require(shrunk && file_state.size == 513,
          "truncate shrink failed");
  std::array<std::byte, 1> eof{};
  auto eof_read = run(*runtime, core->read(kInode, 513, eof));
  require(eof_read && eof_read.value() == 0,
          "read past shrunken EOF returned data");

  reset_virtual(0);
  fail_allocation.store(true, std::memory_order_release);
  std::array<std::byte, 1> one{std::byte{1}};
  auto no_space = run(*runtime, core->write(kInode, 0, one, false));
  require(!no_space && no_space.error().value() == ENOSPC &&
              file_state.size == 0 && file_state.blocks.empty(),
          "ENOSPC did not roll back the reservation");
  fail_allocation.store(false, std::memory_order_release);

  reset_ssd(kBlockSize);
  device_error.store(EIO, std::memory_order_release);
  auto failed_read = run(*runtime, core->read(kInode, 0, one));
  require(!failed_read && failed_read.error().value() == EIO,
          "device submission failure was not propagated through the core");
  device_error.store(0, std::memory_order_release);
  auto synced = run(*runtime, core->sync(kInode));
  require(static_cast<bool>(synced), "core sync failed");

  reset_ssd(2 * kBlockSize);
  block_query_count.store(0, std::memory_order_release);
  (void)read_file(*runtime, core, 2 * kBlockSize);
  const auto queries_after_publish =
      block_query_count.load(std::memory_order_acquire);
  require(queries_after_publish == 2,
          "extent snapshot did not publish the complete mapping");
  (void)read_file(*runtime, core, 2 * kBlockSize);
  require(block_query_count.load(std::memory_order_acquire) ==
              queries_after_publish,
          "steady read traversed the mutable block index");
  std::vector<std::byte> aligned_overwrite(2 * kBlockSize, std::byte{0x5a});
  auto overwritten = run(
      *runtime, core->write(kInode, 0, aligned_overwrite, false));
  require(overwritten && overwritten.value().bytes == aligned_overwrite.size(),
          "aligned extent-cache overwrite failed");
  require(block_query_count.load(std::memory_order_acquire) ==
              queries_after_publish,
          "aligned overwrite traversed the mutable block index");

  reset_virtual(kBlockSize);
  block_query_count.store(0, std::memory_order_release);
  (void)read_file(*runtime, core, kBlockSize);
  const auto virtual_queries =
      block_query_count.load(std::memory_order_acquire);
  std::array<std::byte, kPageSize> partial_virtual{};
  std::fill(partial_virtual.begin(), partial_virtual.end(), std::byte{0x4c});
  auto partial_overwrite = run(
      *runtime, core->write(kInode, kPageSize, partial_virtual, false));
  require(partial_overwrite &&
              partial_overwrite.value().bytes == partial_virtual.size(),
          "partial virtual extent-cache overwrite failed");
  require(block_query_count.load(std::memory_order_acquire) == virtual_queries,
          "partial virtual overwrite traversed the mutable block index");
  std::vector<std::byte> partial_readback(partial_virtual.size());
  auto partial_read = run(
      *runtime, core->read(kInode, kPageSize, partial_readback));
  require(partial_read && partial_readback ==
                              std::vector<std::byte>(partial_virtual.begin(),
                                                     partial_virtual.end()),
          "partial virtual overwrite readback differed");

  for (std::size_t offset = 0; offset < kBlockSize; ++offset) {
    pmem[offset] = pattern(offset);
  }
  migration_candidate.store(true, std::memory_order_release);
  auto migrated = run(*runtime, core->migrate(1));
  require(migrated && !migrated.value(), "migration queue did not drain");
  require(file_state.blocks.front().type == ORCHFS_CORE_SSD_BLOCK &&
              file_state.blocks.front().ssd_offset ==
                  static_cast<std::int64_t>(2 * kBlockSize),
          "migration did not publish the SSD mapping");
  require(std::equal(pmem.begin(), pmem.begin() + kBlockSize,
                     device.begin() + 2 * kBlockSize),
          "migration device write did not preserve block data");
  const auto migrated_bytes = read_file(*runtime, core, kBlockSize);
  require(std::equal(migrated_bytes.begin(), migrated_bytes.end(),
                     pmem.begin()),
          "migrated SSD block readback differed from NVM data");

  auto idle = run(*runtime, core->migrate(1));
  require(idle && !idle.value(), "empty migration queue was not idle");

  core.reset();
  runtime->request_stop();
  require(static_cast<bool>(runtime->join()), "Runtime shutdown failed");
  runtime.reset();

  orchfs::async::RuntimeOptions single_options;
  single_options.worker_count = 1;
  auto single_created =
      orchfs::async::Runtime::create(std::move(single_options));
  require(static_cast<bool>(single_created),
          "single-worker Runtime create failed");
  auto single_runtime = std::move(single_created).value();
  auto single_core_result =
      orchfs::async::KfsCoroutineCore::create(*single_runtime);
  require(static_cast<bool>(single_core_result),
          "single-worker KFS core create failed");
  auto single_core = std::move(single_core_result).value();
  reset_virtual(0);
  std::atomic<bool> stop_heartbeat{false};
  auto heartbeat_submitted = single_runtime->submit(
      heartbeat(stop_heartbeat));
  auto truncate_submitted = single_runtime->submit(
      single_core->truncate(kInode, 2 * 1024 * 1024 + 513));
  require(heartbeat_submitted && truncate_submitted,
          "single-worker progress tasks were not submitted");
  auto truncate_joined = std::move(truncate_submitted).value().join();
  require(truncate_joined && truncate_joined.value(),
          "single-worker truncate did not complete");
  stop_heartbeat.store(true, std::memory_order_release);
  auto heartbeat_joined = std::move(heartbeat_submitted).value().join();
  require(heartbeat_joined && heartbeat_joined.value() &&
              heartbeat_joined.value().value() > 1,
          "single-worker heartbeat did not progress during complex I/O");
  single_core.reset();
  single_runtime->request_stop();
  require(static_cast<bool>(single_runtime->join()),
          "single-worker Runtime shutdown failed");
  orchfs_repro_trace_flush();
  return 0;
}
