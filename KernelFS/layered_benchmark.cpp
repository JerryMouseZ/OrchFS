#include "async_server_bridge_internal.hpp"
#include "kernel_func.h"
#include "spdk_device_service.h"

#include "orchfs/async/block_device.hpp"
#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/async/kfs_coroutine_core.hpp"
#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <limits>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <unistd.h>

namespace {

constexpr std::size_t kBlockSize = 64U * 1024U;
constexpr std::size_t kStreamCount = 4;
constexpr std::size_t kDefaultCoroutineCount = 64;
constexpr std::size_t kDefaultOperationCount = 4U * 4096U;
constexpr std::uint64_t kFilesystemDeviceEnd = std::uint64_t{1} << 40U;

using Latency = std::chrono::nanoseconds;

struct BenchmarkConfig {
  std::size_t coroutine_count{kDefaultCoroutineCount};
  std::size_t operation_count{kDefaultOperationCount};
  std::uint64_t device_offset{};
};

struct BenchmarkSample {
  std::uint64_t bytes{};
  std::chrono::duration<double> elapsed{};
};

int error_number(std::error_code error) noexcept {
  if (!error) {
    return 0;
  }
  return orchfs::async::detail::errno_error(error.value()).value();
}

template <typename Integer>
int parse_integer(const char* name, Integer default_value, Integer minimum,
                  Integer maximum, Integer& output, bool required = false) {
  const char* text = std::getenv(name);
  if (text == nullptr || *text == '\0') {
    if (required) {
      std::fprintf(stderr, "%s is required\n", name);
      return EINVAL;
    }
    output = default_value;
    return 0;
  }
  const std::string_view input(text);
  Integer parsed{};
  const auto [end, error] =
      std::from_chars(input.data(), input.data() + input.size(), parsed);
  if (error != std::errc{} || end != input.data() + input.size() ||
      parsed < minimum || parsed > maximum) {
    std::fprintf(stderr, "invalid %s='%s'\n", name, text);
    return EINVAL;
  }
  output = parsed;
  return 0;
}

int load_config(orchfs::async::Runtime& runtime, BenchmarkConfig& config) {
  int error = parse_integer(
      "ORCHFS_ASYNC_BENCHMARK_COROUTINES", kDefaultCoroutineCount,
      std::size_t{1}, std::numeric_limits<std::size_t>::max(),
      config.coroutine_count);
  if (error == 0) {
    error = parse_integer(
        "ORCHFS_LAYERED_BENCHMARK_OPERATIONS", kDefaultOperationCount,
        std::size_t{1}, std::numeric_limits<std::size_t>::max(),
        config.operation_count);
  }
  if (error == 0) {
    error = parse_integer(
        "ORCHFS_LAYERED_BENCHMARK_DEVICE_OFFSET", std::uint64_t{0},
        std::uint64_t{0}, std::numeric_limits<std::uint64_t>::max(),
        config.device_offset, true);
  }
  if (error != 0) {
    return error;
  }
  if (runtime.worker_count() != kStreamCount ||
      config.coroutine_count < kStreamCount ||
      config.coroutine_count % kStreamCount != 0 ||
      config.operation_count % config.coroutine_count != 0) {
    std::fprintf(stderr,
                 "layered benchmark requires 4 Runtime workers, a coroutine "
                 "count divisible by 4, and operations divisible by "
                 "coroutines\n");
    return EINVAL;
  }
  if (config.operation_count >
      std::numeric_limits<std::uint64_t>::max() / kBlockSize) {
    return EOVERFLOW;
  }
  const std::uint64_t benchmark_bytes =
      static_cast<std::uint64_t>(config.operation_count) * kBlockSize;
  const std::uint64_t capacity = orchfs_spdk_device_capacity_bytes();
  if (config.device_offset < kFilesystemDeviceEnd ||
      config.device_offset % kBlockSize != 0 || capacity == 0 ||
      config.device_offset > capacity ||
      benchmark_bytes > capacity - config.device_offset) {
    std::fprintf(
        stderr,
        "direct device range must be 64 KiB aligned, start at or above "
        "the 1 TiB OrchFS format boundary, and fit in the namespace "
        "(offset=%llu bytes=%llu capacity=%llu)\n",
        static_cast<unsigned long long>(config.device_offset),
        static_cast<unsigned long long>(benchmark_bytes),
        static_cast<unsigned long long>(capacity));
    return ERANGE;
  }
  return 0;
}

template <typename T>
orchfs::async::Result<T> run_one(
    orchfs::async::Runtime& runtime,
    orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  if (!submitted) {
    return orchfs::async::Result<T>::failure(submitted.error());
  }
  auto joined = std::move(submitted).value().join();
  if (!joined) {
    return orchfs::async::Result<T>::failure(joined.error());
  }
  return std::move(joined).value();
}

template <typename MakeTask>
orchfs::async::Result<BenchmarkSample> run_phase(
    orchfs::async::Runtime& runtime, std::size_t coroutine_count,
    MakeTask make_task) {
  using RootResult = orchfs::async::Result<std::uint64_t>;
  std::vector<orchfs::async::JoinHandle<RootResult>> handles;
  try {
    handles.reserve(coroutine_count);
  } catch (const std::bad_alloc&) {
    return orchfs::async::Result<BenchmarkSample>::failure(
        std::make_error_code(std::errc::not_enough_memory));
  }

  std::error_code first_error;
  const auto started = std::chrono::steady_clock::now();
  for (std::size_t coroutine = 0; coroutine < coroutine_count; ++coroutine) {
    auto submitted = runtime.submit(make_task(coroutine));
    if (!submitted) {
      first_error = submitted.error();
      break;
    }
    handles.push_back(std::move(submitted).value());
  }

  std::uint64_t bytes = 0;
  for (auto& handle : handles) {
    auto joined = std::move(handle).join();
    if (!joined) {
      if (!first_error) {
        first_error = joined.error();
      }
      continue;
    }
    auto result = std::move(joined).value();
    if (!result) {
      if (!first_error) {
        first_error = result.error();
      }
      continue;
    }
    bytes += result.value();
  }
  if (first_error) {
    return orchfs::async::Result<BenchmarkSample>::failure(first_error);
  }
  return orchfs::async::Result<BenchmarkSample>::success(BenchmarkSample{
      .bytes = bytes,
      .elapsed = std::chrono::steady_clock::now() - started,
  });
}

template <typename MakeTask>
orchfs::async::Result<std::chrono::duration<double>> run_void_phase(
    orchfs::async::Runtime& runtime, std::size_t task_count,
    MakeTask make_task) {
  using RootResult = orchfs::async::Result<void>;
  std::vector<orchfs::async::JoinHandle<RootResult>> handles;
  try {
    handles.reserve(task_count);
  } catch (const std::bad_alloc&) {
    return orchfs::async::Result<std::chrono::duration<double>>::failure(
        std::make_error_code(std::errc::not_enough_memory));
  }

  std::error_code first_error;
  const auto started = std::chrono::steady_clock::now();
  for (std::size_t index = 0; index < task_count; ++index) {
    auto submitted = runtime.submit(make_task(index));
    if (!submitted) {
      first_error = submitted.error();
      break;
    }
    handles.push_back(std::move(submitted).value());
  }
  for (auto& handle : handles) {
    auto joined = std::move(handle).join();
    if (!joined) {
      if (!first_error) {
        first_error = joined.error();
      }
      continue;
    }
    auto result = std::move(joined).value();
    if (!result && !first_error) {
      first_error = result.error();
    }
  }
  if (first_error) {
    return orchfs::async::Result<std::chrono::duration<double>>::failure(
        first_error);
  }
  return orchfs::async::Result<std::chrono::duration<double>>::success(
      std::chrono::steady_clock::now() - started);
}

void print_sample(std::string_view layer, std::string_view phase,
                  const BenchmarkSample& sample,
                  std::size_t coroutine_count,
                  const std::vector<std::vector<Latency>>& latencies) {
  std::vector<Latency> flattened;
  std::size_t sample_count = 0;
  for (const auto& values : latencies) {
    sample_count += values.size();
  }
  flattened.reserve(sample_count);
  for (const auto& values : latencies) {
    flattened.insert(flattened.end(), values.begin(), values.end());
  }

  double p99_microseconds = 0.0;
  if (!flattened.empty()) {
    const std::size_t percentile =
        (flattened.size() * 99U + 99U) / 100U - 1U;
    std::nth_element(flattened.begin(), flattened.begin() + percentile,
                     flattened.end());
    p99_microseconds =
        std::chrono::duration<double, std::micro>(flattened[percentile])
            .count();
  }

  const double seconds = sample.elapsed.count();
  const double mebibytes =
      static_cast<double>(sample.bytes) / (1024.0 * 1024.0);
  const double operations = static_cast<double>(sample.bytes) / kBlockSize;
  std::printf(
      "orchfs_layered_benchmark layer=%.*s phase=%.*s block_size=%zu "
      "qd=%zu coroutines=%zu bytes=%llu seconds=%.6f MiB_per_s=%.2f "
      "IOPS=%.2f p99_us=%.2f latency_samples=%zu\n",
      static_cast<int>(layer.size()), layer.data(),
      static_cast<int>(phase.size()), phase.data(), kBlockSize,
      coroutine_count, coroutine_count,
      static_cast<unsigned long long>(sample.bytes), seconds,
      mebibytes / seconds, operations / seconds, p99_microseconds,
      flattened.size());
}

orchfs::async::Task<orchfs::async::Result<std::uint64_t>> device_io_task(
    orchfs::async::AsyncBlockDevice& device, bool write,
    std::span<std::byte> buffer, std::size_t operations,
    std::uint64_t first_offset, std::uint64_t stride,
    std::vector<Latency>* latencies) {
  std::uint64_t bytes = 0;
  for (std::size_t operation = 0; operation < operations; ++operation) {
    const std::uint64_t offset = first_offset + operation * stride;
    const auto started = std::chrono::steady_clock::now();
    auto completed = write
        ? co_await device.write(offset, std::span<const std::byte>(buffer))
        : co_await device.read(offset, buffer);
    if (latencies != nullptr) {
      latencies->push_back(std::chrono::duration_cast<Latency>(
          std::chrono::steady_clock::now() - started));
    }
    if (!completed) {
      co_return orchfs::async::Result<std::uint64_t>::failure(
          completed.error());
    }
    if (completed.value() != buffer.size()) {
      co_return orchfs::async::Result<std::uint64_t>::failure(
          std::make_error_code(std::errc::io_error));
    }
    bytes += completed.value();
  }
  co_return orchfs::async::Result<std::uint64_t>::success(bytes);
}

int benchmark_device(orchfs::async::Runtime& runtime,
                     const BenchmarkConfig& config) {
  orchfs::async::AsyncBlockDevice device(runtime);
  const std::size_t operations_per_coroutine =
      config.operation_count / config.coroutine_count;
  const std::uint64_t stride =
      static_cast<std::uint64_t>(config.coroutine_count) * kBlockSize;

  std::vector<std::vector<std::byte>> write_buffers;
  std::vector<std::vector<std::byte>> read_buffers;
  std::vector<std::vector<Latency>> write_latencies;
  std::vector<std::vector<Latency>> read_latencies;
  try {
    write_buffers.resize(config.coroutine_count);
    read_buffers.resize(config.coroutine_count);
    write_latencies.resize(config.coroutine_count);
    read_latencies.resize(config.coroutine_count);
    for (std::size_t coroutine = 0; coroutine < config.coroutine_count;
         ++coroutine) {
      const auto pattern = static_cast<std::byte>(
          static_cast<unsigned char>(coroutine % 251U + 1U));
      write_buffers[coroutine].assign(kBlockSize, pattern);
      read_buffers[coroutine].resize(kBlockSize);
      write_latencies[coroutine].reserve(operations_per_coroutine);
      read_latencies[coroutine].reserve(operations_per_coroutine);
    }
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  }

  auto warm_write = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return device_io_task(
            device, true, write_buffers[coroutine], 1,
            config.device_offset + coroutine * kBlockSize, stride, nullptr);
      });
  if (!warm_write) {
    return error_number(warm_write.error());
  }
  auto warmed_flush = run_one(runtime, device.flush());
  if (!warmed_flush) {
    return error_number(warmed_flush.error());
  }

  auto write = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return device_io_task(
            device, true, write_buffers[coroutine], operations_per_coroutine,
            config.device_offset + coroutine * kBlockSize, stride,
            &write_latencies[coroutine]);
      });
  if (!write) {
    return error_number(write.error());
  }
  const auto flush_started = std::chrono::steady_clock::now();
  auto flushed = run_one(runtime, device.flush());
  if (!flushed) {
    return error_number(flushed.error());
  }
  write.value().elapsed += std::chrono::steady_clock::now() - flush_started;

  auto warm_read = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return device_io_task(
            device, false, read_buffers[coroutine], 1,
            config.device_offset + coroutine * kBlockSize, stride, nullptr);
      });
  if (!warm_read) {
    return error_number(warm_read.error());
  }
  auto read = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return device_io_task(
            device, false, read_buffers[coroutine], operations_per_coroutine,
            config.device_offset + coroutine * kBlockSize, stride,
            &read_latencies[coroutine]);
      });
  if (!read) {
    return error_number(read.error());
  }
  for (std::size_t coroutine = 0; coroutine < config.coroutine_count;
       ++coroutine) {
    const auto expected = static_cast<std::byte>(
        static_cast<unsigned char>(coroutine % 251U + 1U));
    if (!std::all_of(read_buffers[coroutine].begin(),
                     read_buffers[coroutine].end(),
                     [expected](std::byte value) {
                       return value == expected;
                     })) {
      return EIO;
    }
  }

  print_sample("async_block_device_direct", "write+sync", write.value(),
               config.coroutine_count, write_latencies);
  print_sample("async_block_device_direct", "read", read.value(),
               config.coroutine_count, read_latencies);
  return 0;
}

orchfs::async::Task<orchfs::async::Result<std::uint64_t>> core_io_task(
    orchfs::async::KfsCoroutineCore& core, orchfs::async::InodeNumber inode,
    bool write, std::span<std::byte> buffer, std::size_t operations,
    std::uint64_t first_offset, std::uint64_t stride,
    std::vector<Latency>* latencies) {
  std::uint64_t bytes = 0;
  for (std::size_t operation = 0; operation < operations; ++operation) {
    const std::uint64_t offset = first_offset + operation * stride;
    const auto started = std::chrono::steady_clock::now();
    orchfs::async::Result<std::size_t> completed =
        orchfs::async::Result<std::size_t>::failure(
            std::make_error_code(std::errc::io_error));
    if (write) {
      auto written = co_await core.write(
          inode, offset, std::span<const std::byte>(buffer), false);
      completed = written
          ? orchfs::async::Result<std::size_t>::success(
                written.value().bytes)
          : orchfs::async::Result<std::size_t>::failure(written.error());
    } else {
      completed = co_await core.read(inode, offset, buffer);
    }
    if (latencies != nullptr) {
      latencies->push_back(std::chrono::duration_cast<Latency>(
          std::chrono::steady_clock::now() - started));
    }
    if (!completed || completed.value() != buffer.size()) {
      co_return completed
          ? orchfs::async::Result<std::uint64_t>::failure(
                std::make_error_code(std::errc::io_error))
          : orchfs::async::Result<std::uint64_t>::failure(completed.error());
    }
    bytes += completed.value();
  }
  co_return orchfs::async::Result<std::uint64_t>::success(bytes);
}

class CoreFiles final {
 public:
  CoreFiles(orchfs::async::Runtime& runtime,
            std::shared_ptr<orchfs::async::KfsCoroutineCore> core)
      : runtime_(&runtime), core_(std::move(core)) {}

  ~CoreFiles() { cleanup(); }

  CoreFiles(const CoreFiles&) = delete;
  CoreFiles& operator=(const CoreFiles&) = delete;

  int open_all() {
    try {
      nodes_.reserve(kStreamCount);
      paths_.reserve(kStreamCount);
      for (std::size_t stream = 0; stream < kStreamCount; ++stream) {
        paths_.push_back("/.orchfs-layered-direct-" +
                         std::to_string(static_cast<unsigned long>(::getpid())) +
                         "-" + std::to_string(stream));
        auto opened = run_one(
            *runtime_, core_->open(paths_.back(), O_CREAT | O_RDWR | O_TRUNC,
                                   0600));
        if (!opened) {
          return error_number(opened.error());
        }
        nodes_.push_back(opened.value());
      }
    } catch (const std::bad_alloc&) {
      return ENOMEM;
    }
    return 0;
  }

  const std::vector<orchfs::async::OpenedNode>& nodes() const noexcept {
    return nodes_;
  }

  int cleanup() noexcept {
    if (cleaned_) {
      return cleanup_error_;
    }
    cleaned_ = true;
    for (const auto& node : nodes_) {
      auto closed = run_one(*runtime_, core_->close(node));
      if (!closed && cleanup_error_ == 0) {
        cleanup_error_ = error_number(closed.error());
      }
    }
    for (const auto& path : paths_) {
      auto removed = run_one(*runtime_, core_->unlink(path));
      if (!removed && cleanup_error_ == 0) {
        cleanup_error_ = error_number(removed.error());
      }
    }
    nodes_.clear();
    paths_.clear();
    return cleanup_error_;
  }

 private:
  orchfs::async::Runtime* runtime_{};
  std::shared_ptr<orchfs::async::KfsCoroutineCore> core_;
  std::vector<orchfs::async::OpenedNode> nodes_;
  std::vector<std::string> paths_;
  int cleanup_error_{};
  bool cleaned_{};
};

int benchmark_core(
    orchfs::async::Runtime& runtime,
    const std::shared_ptr<orchfs::async::KfsCoroutineCore>& core,
    const BenchmarkConfig& config) {
  CoreFiles files(runtime, core);
  int error = files.open_all();
  if (error != 0) {
    return error;
  }
  const auto& nodes = files.nodes();
  const std::size_t operations_per_coroutine =
      config.operation_count / config.coroutine_count;
  const std::size_t coroutines_per_stream =
      config.coroutine_count / kStreamCount;
  const std::uint64_t stride =
      static_cast<std::uint64_t>(coroutines_per_stream) * kBlockSize;
  const std::uint64_t file_size =
      static_cast<std::uint64_t>(config.operation_count / kStreamCount) *
      kBlockSize;

  for (const auto& node : nodes) {
    auto truncated = run_one(runtime, core->truncate(node.inode, file_size));
    if (!truncated) {
      return error_number(truncated.error());
    }
  }

  std::vector<std::vector<std::byte>> write_buffers;
  std::vector<std::vector<std::byte>> read_buffers;
  std::vector<std::vector<Latency>> write_latencies;
  std::vector<std::vector<Latency>> read_latencies;
  try {
    write_buffers.resize(config.coroutine_count);
    read_buffers.resize(config.coroutine_count);
    write_latencies.resize(config.coroutine_count);
    read_latencies.resize(config.coroutine_count);
    for (std::size_t coroutine = 0; coroutine < config.coroutine_count;
         ++coroutine) {
      const std::size_t stream = coroutine % kStreamCount;
      const auto pattern = static_cast<std::byte>(
          static_cast<unsigned char>(stream + 1U));
      write_buffers[coroutine].assign(kBlockSize, pattern);
      read_buffers[coroutine].resize(kBlockSize);
      write_latencies[coroutine].reserve(operations_per_coroutine);
      read_latencies[coroutine].reserve(operations_per_coroutine);
    }
  } catch (const std::bad_alloc&) {
    return ENOMEM;
  }

  auto make_task = [&](bool write, std::size_t coroutine,
                       std::size_t operations,
                       std::vector<Latency>* latencies) {
    const std::size_t stream = coroutine % kStreamCount;
    const std::size_t lane = coroutine / kStreamCount;
    auto& buffer = write ? write_buffers[coroutine] : read_buffers[coroutine];
    return core_io_task(*core, nodes[stream].inode, write, buffer, operations,
                        lane * kBlockSize, stride, latencies);
  };

  // Allocate every extent before the sample. The measured write is therefore
  // a steady aligned overwrite, matching the journal fast-path workload.
  auto populated = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return make_task(true, coroutine, operations_per_coroutine, nullptr);
      });
  if (!populated) {
    return error_number(populated.error());
  }
  auto initial_sync = run_void_phase(
      runtime, kStreamCount, [&](std::size_t stream) {
        return core->sync(nodes[stream].inode);
      });
  if (!initial_sync) {
    return error_number(initial_sync.error());
  }

  auto write = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return make_task(true, coroutine, operations_per_coroutine,
                         &write_latencies[coroutine]);
      });
  if (!write) {
    return error_number(write.error());
  }
  auto synced = run_void_phase(
      runtime, kStreamCount, [&](std::size_t stream) {
        return core->sync(nodes[stream].inode);
      });
  if (!synced) {
    return error_number(synced.error());
  }
  write.value().elapsed += synced.value();

  auto warm_read = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return make_task(false, coroutine, 1, nullptr);
      });
  if (!warm_read) {
    return error_number(warm_read.error());
  }
  auto read = run_phase(
      runtime, config.coroutine_count, [&](std::size_t coroutine) {
        return make_task(false, coroutine, operations_per_coroutine,
                         &read_latencies[coroutine]);
      });
  if (!read) {
    return error_number(read.error());
  }
  for (std::size_t coroutine = 0; coroutine < config.coroutine_count;
       ++coroutine) {
    const std::size_t stream = coroutine % kStreamCount;
    const auto expected = static_cast<std::byte>(
        static_cast<unsigned char>(stream + 1U));
    if (!std::all_of(read_buffers[coroutine].begin(),
                     read_buffers[coroutine].end(),
                     [expected](std::byte value) {
                       return value == expected;
                     })) {
      return EIO;
    }
  }

  print_sample("kfs_coroutine_core_direct", "write+sync", write.value(),
               config.coroutine_count, write_latencies);
  print_sample("kfs_coroutine_core_direct", "read", read.value(),
               config.coroutine_count, read_latencies);
  return files.cleanup();
}

int run_benchmarks() {
  auto context = orchfs::kfs::async_context_snapshot();
  if (!context || context->runtime == nullptr || !context->filesystem) {
    return ENODEV;
  }
  BenchmarkConfig config;
  int error = load_config(*context->runtime, config);
  if (error == 0) {
    error = benchmark_device(*context->runtime, config);
  }
  if (error == 0) {
    error = benchmark_core(*context->runtime, context->filesystem, config);
  }
  return error;
}

}  // namespace

int main() {
  init_kernelFS_direct();
  const int error = run_benchmarks();
  if (error != 0) {
    std::fprintf(stderr, "OrchFS layered direct benchmark failed: %s (%d)\n",
                 std::strerror(error), error);
  }
  close_kernelFS();
  return error == 0 ? 0 : 1;
}
