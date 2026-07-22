#include "orchfs/async/block_device.hpp"
#include "orchfs/async/runtime.hpp"

extern "C" {
#include "../KernelFS/async_device.h"
}

#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <span>
#include <thread>
#include <utility>

namespace {

using Completion = void (*)(void*, int, std::size_t);

enum class DeviceMode : int {
  success,
  completion_failure,
  short_completion,
  fail_second_submission,
  pend_first_fail_second,
  pending,
};

struct PendingCompletion {
  std::atomic<bool> ready{false};
  Completion completion{};
  void* context{};
  std::size_t bytes{};
};

struct DeviceControl {
  std::atomic<DeviceMode> mode{DeviceMode::success};
  std::atomic<int> calls{0};
  PendingCompletion pending;
};

DeviceControl control;

[[noreturn]] void fail(const char* message) {
  std::fprintf(stderr, "async block device test failure: %s\n", message);
  std::abort();
}

void require(bool condition, const char* message) {
  if (!condition) {
    fail(message);
  }
}

void reset(DeviceMode mode) {
  require(!control.pending.ready.load(std::memory_order_acquire),
          "reset with a pending completion");
  control.calls.store(0, std::memory_order_release);
  control.mode.store(mode, std::memory_order_release);
}

int submit(std::size_t length, Completion completion, void* context) {
  const int call = control.calls.fetch_add(1, std::memory_order_acq_rel);
  const auto mode = control.mode.load(std::memory_order_acquire);
  if ((mode == DeviceMode::fail_second_submission ||
       mode == DeviceMode::pend_first_fail_second) &&
      call == 1) {
    return EAGAIN;
  }
  if (mode == DeviceMode::pending ||
      (mode == DeviceMode::pend_first_fail_second && call == 0)) {
    control.pending.completion = completion;
    control.pending.context = context;
    control.pending.bytes = length;
    control.pending.ready.store(true, std::memory_order_release);
    control.pending.ready.notify_all();
    return 0;
  }
  if (mode == DeviceMode::completion_failure) {
    completion(context, EIO, 0);
  } else if (mode == DeviceMode::short_completion) {
    completion(context, 0, length == 0 ? 1 : length - 1);
  } else {
    completion(context, 0, length);
  }
  return 0;
}

void wait_for_pending() {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!control.pending.ready.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      fail("device request was not submitted");
    }
    std::this_thread::yield();
  }
}

void wait_for_calls(int expected) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (control.calls.load(std::memory_order_acquire) < expected) {
    if (std::chrono::steady_clock::now() >= deadline) {
      fail("device batch did not submit every request");
    }
    std::this_thread::yield();
  }
}

void complete_pending(int error = 0) {
  wait_for_pending();
  const auto completion = control.pending.completion;
  void* const context = control.pending.context;
  const auto bytes = control.pending.bytes;
  control.pending.ready.store(false, std::memory_order_release);
  completion(context, error, error == 0 ? bytes : 0);
}

template <typename T>
orchfs::async::Result<T> join(
    orchfs::async::JoinHandle<orchfs::async::Result<T>> handle) {
  auto joined = std::move(handle).join();
  require(static_cast<bool>(joined), "JoinHandle::join failed");
  return std::move(joined).value();
}

template <typename T>
orchfs::async::JoinHandle<orchfs::async::Result<T>> submit_task(
    orchfs::async::Runtime& runtime,
    orchfs::async::Task<orchfs::async::Result<T>> task) {
  auto submitted = runtime.submit(std::move(task));
  require(static_cast<bool>(submitted), "Runtime::submit failed");
  return std::move(submitted).value();
}

}  // namespace

extern "C" int submit_read_data_from_devs(
    void*, std::int64_t length, std::int64_t, Completion completion,
    void* context) {
  return submit(static_cast<std::size_t>(length), completion, context);
}

extern "C" int submit_write_data_to_devs(
    const void*, std::int64_t length, std::int64_t, Completion completion,
    void* context) {
  return submit(static_cast<std::size_t>(length), completion, context);
}

extern "C" int submit_device_sync(Completion completion, void* context) {
  return submit(0, completion, context);
}

extern "C" int orchfs_device_effective_write_durability() {
  return ORCHFS_DEVICE_DURABILITY_COMPLETION;
}

int main() {
  orchfs::async::RuntimeOptions options;
  options.worker_count = 1;
  auto created = orchfs::async::Runtime::create(std::move(options));
  require(static_cast<bool>(created), "Runtime::create failed");
  auto runtime = std::move(created).value();
  orchfs::async::AsyncBlockDevice device(*runtime);

  std::array<std::byte, 32> first{};
  std::array<std::byte, 17> second{};

  reset(DeviceMode::success);
  auto read = join(submit_task(*runtime, device.read(7, first)));
  require(read && read.value() == first.size(), "inline read failed");
  auto write = join(submit_task(
      *runtime, device.write(11, std::span<const std::byte>(second))));
  require(write && write.value() == second.size(), "inline write failed");
  auto flush = join(submit_task(*runtime, device.flush()));
  require(static_cast<bool>(flush), "inline flush failed");

  reset(DeviceMode::success);
  const std::array reads{
      orchfs::async::BlockRead{.offset = 0, .destination = first},
      orchfs::async::BlockRead{.offset = 4096, .destination = second},
  };
  auto batch = join(submit_task(*runtime, device.read_batch(reads)));
  require(batch && batch.value() == first.size() + second.size(),
          "successful batch returned the wrong byte count");

  std::array<std::byte, 64> contiguous{};
  const std::array adjacent_reads{
      orchfs::async::BlockRead{
          .offset = 8192,
          .destination = std::span(contiguous).first<32>(),
      },
      orchfs::async::BlockRead{
          .offset = 8192 + 32,
          .destination = std::span(contiguous).subspan<32>(),
      },
  };
  reset(DeviceMode::success);
  auto merged = join(submit_task(*runtime, device.read_batch(adjacent_reads)));
  require(merged && merged.value() == contiguous.size(),
          "merged batch returned the wrong byte count");
  require(control.calls.load(std::memory_order_acquire) == 1,
          "adjacent device requests were not merged");

  reset(DeviceMode::completion_failure);
  auto failed = join(submit_task(*runtime, device.read(0, first)));
  require(!failed && failed.error().value() == EIO,
          "completion failure was not propagated");

  reset(DeviceMode::short_completion);
  auto short_read = join(submit_task(*runtime, device.read(0, first)));
  require(!short_read && short_read.error().value() == EIO,
          "short single completion was accepted");
  auto short_batch = join(submit_task(*runtime, device.read_batch(reads)));
  require(!short_batch && short_batch.error().value() == EIO,
          "short batch completion was accepted");

  reset(DeviceMode::fail_second_submission);
  auto submit_failure =
      join(submit_task(*runtime, device.read_batch(reads)));
  require(!submit_failure && submit_failure.error().value() == EAGAIN,
          "batch submit failure was not propagated");

  reset(DeviceMode::pend_first_fail_second);
  auto partial = submit_task(*runtime, device.read_batch(reads));
  wait_for_pending();
  wait_for_calls(2);
  require(control.calls.load(std::memory_order_acquire) == 2,
          "batch did not reach the failing submission");
  require(!partial.ready(),
          "batch resumed before an accepted request completed");
  complete_pending();
  auto partial_result = join(std::move(partial));
  require(!partial_result && partial_result.error().value() == EAGAIN,
          "partial batch lost its submission error");

  reset(DeviceMode::pending);
  auto draining = submit_task(*runtime, device.read(0, first));
  wait_for_pending();
  runtime->request_stop();
  require(!draining.ready(), "Runtime abandoned pending device I/O");
  complete_pending();
  auto drained = join(std::move(draining));
  require(drained && drained.value() == first.size(),
          "pending I/O did not drain during shutdown");
  require(static_cast<bool>(runtime->join()), "Runtime::join failed");
  return 0;
}
