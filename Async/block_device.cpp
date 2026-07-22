#include "orchfs/async/block_device.hpp"

#include "orchfs/async/detail/concurrency.hpp"
#include "orchfs/async/repro_trace.hpp"
#include "../KernelFS/async_device.h"

#include <cerrno>
#include <array>
#include <atomic>
#include <coroutine>
#include <limits>
#include <new>
#include <optional>
#include <span>
#include <system_error>
#include <type_traits>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace {

enum class Operation { read, write, flush };

struct BatchRequest {
    std::uint64_t offset{};
    void* buffer{};
    std::size_t length{};
};

constexpr std::size_t kMaximumMergedTransfer = 256U * 1024U;
constexpr std::size_t kInlineBatchRequests = 8;

class BatchRequestList final {
public:
    explicit BatchRequestList(std::size_t expected) {
        if (expected > inline_requests_.size()) {
            overflow_.reserve(expected);
        }
    }

    [[nodiscard]] bool empty() const noexcept { return size() == 0; }
    [[nodiscard]] std::size_t size() const noexcept {
        return overflow_.capacity() == 0 ? inline_size_ : overflow_.size();
    }
    BatchRequest& back() noexcept {
        return overflow_.capacity() == 0
            ? inline_requests_[inline_size_ - 1] : overflow_.back();
    }
    void push_back(BatchRequest request) {
        if (overflow_.capacity() == 0) {
            inline_requests_[inline_size_++] = request;
        } else {
            overflow_.push_back(request);
        }
    }
    [[nodiscard]] std::span<const BatchRequest> span() const noexcept {
        return overflow_.capacity() == 0
            ? std::span<const BatchRequest>(inline_requests_.data(), inline_size_)
            : std::span<const BatchRequest>(overflow_);
    }

private:
    std::array<BatchRequest, kInlineBatchRequests> inline_requests_{};
    std::size_t inline_size_{};
    std::vector<BatchRequest> overflow_;
};

void append_merged_request(BatchRequestList& requests,
                           BatchRequest request) {
    if (!requests.empty() && request.length != 0) {
        auto& previous = requests.back();
        const auto previous_address =
            reinterpret_cast<std::uintptr_t>(previous.buffer);
        const auto request_address =
            reinterpret_cast<std::uintptr_t>(request.buffer);
        const bool offset_adjacent =
            previous.offset <=
                std::numeric_limits<std::uint64_t>::max() - previous.length &&
            previous.offset + previous.length == request.offset;
        const bool buffer_adjacent =
            previous_address <=
                std::numeric_limits<std::uintptr_t>::max() - previous.length &&
            previous_address + previous.length == request_address;
        const bool size_allowed =
            request.length <= kMaximumMergedTransfer &&
            previous.length <= kMaximumMergedTransfer - request.length;
        if (offset_adjacent && buffer_adjacent && size_allowed) {
            previous.length += request.length;
            return;
        }
    }
    requests.push_back(request);
}

template <bool kBatch>
class BatchDeviceAwaiter;

struct BatchCompletion {
    BatchDeviceAwaiter<true>* awaiter{};
    std::size_t expected{};
};

struct SingleRequestStorage {
    BatchRequest request;
};

struct BatchRequestStorage {
    std::span<const BatchRequest> requests;
    std::span<BatchCompletion> completions;
};

struct SingleCompletionState {
    std::error_code error;
    std::size_t bytes{};
};

struct BatchCompletionState {
    std::atomic<std::size_t> outstanding{1};
    std::atomic<std::size_t> bytes{0};
    std::atomic<int> error{0};
};

template <bool kBatch>
class BatchDeviceAwaiter final {
public:
    enum class SubmissionState : std::uint8_t {
        submitting,
        suspended,
        completed,
    };

    BatchDeviceAwaiter(Runtime& runtime, Operation operation,
                       BatchRequest request) noexcept
        requires (!kBatch)
        : runtime_(&runtime), operation_(operation),
          requests_{.request = request} {}

    BatchDeviceAwaiter(Runtime& runtime, Operation operation,
                       std::span<const BatchRequest> requests,
                       std::span<BatchCompletion> completions) noexcept
        requires kBatch
        : runtime_(&runtime), operation_(operation),
          requests_{.requests = requests, .completions = completions} {}

    [[nodiscard]] bool await_ready() const noexcept {
        if constexpr (kBatch) {
            return requests_.requests.empty();
        }
        return false;
    }

    [[nodiscard]] bool await_suspend(
        std::coroutine_handle<> continuation) noexcept {
        if (Runtime::current() != runtime_ ||
            Runtime::current_worker() == detail::no_worker) {
            if constexpr (kBatch) {
                completion_.error.store(
                    make_error_code(Errc::not_in_runtime).value(),
                    std::memory_order_relaxed);
            } else {
                completion_.error = make_error_code(Errc::not_in_runtime);
            }
            return false;
        }
        continuation_ = continuation;
        worker_ = Runtime::current_worker();
        constexpr auto max_offset = static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());

        if constexpr (kBatch) {
            if (requests_.completions.size() != requests_.requests.size()) {
                record_error(EINVAL);
                continuation_ = {};
                return false;
            }
            for (std::size_t index = 0; index < requests_.requests.size();
                 ++index) {
                const auto& request = requests_.requests[index];
                if ((request.buffer == nullptr && request.length != 0) ||
                    request.offset > max_offset ||
                    request.length > static_cast<std::size_t>(
                                         max_offset - request.offset)) {
                    record_error(EINVAL);
                    break;
                }
                requests_.completions[index] = BatchCompletion{
                    .awaiter = this,
                    .expected = request.length,
                };
                completion_.outstanding.fetch_add(
                    1, std::memory_order_relaxed);
                const int submitted = submit(
                    request, &requests_.completions[index]);
                if (submitted != 0) {
                    completion_.outstanding.fetch_sub(
                        1, std::memory_order_relaxed);
                    record_error(submitted);
                    break;
                }
            }

            // Drop the submission sentinel after every request that reached
            // the device has installed its callback.
            if (completion_.outstanding.fetch_sub(
                    1, std::memory_order_acq_rel) == 1) {
                submission_state_.store(SubmissionState::completed,
                                        std::memory_order_release);
                continuation_ = {};
                return false;
            }
        } else {
            const auto& request = requests_.request;
            if (request.offset > max_offset ||
                request.length > static_cast<std::size_t>(
                                     max_offset - request.offset)) {
                completion_.error =
                    std::make_error_code(std::errc::value_too_large);
                continuation_ = {};
                return false;
            }
            const int submitted = submit(request, this);
            if (submitted != 0) {
                completion_.error = detail::errno_error<false>(submitted);
                continuation_ = {};
                return false;
            }
        }

        const auto previous = submission_state_.exchange(
            SubmissionState::suspended, std::memory_order_acq_rel);
        if (previous == SubmissionState::completed) {
            continuation_ = {};
            return false;
        }
        if (previous != SubmissionState::submitting) {
            std::terminate();
        }
        return true;
    }

    [[nodiscard]] Result<std::size_t> await_resume() const noexcept {
        if constexpr (kBatch) {
            const int error =
                completion_.error.load(std::memory_order_acquire);
            if (error != 0) {
                return Result<std::size_t>::failure(
                    detail::errno_error<false>(error));
            }
            return Result<std::size_t>::success(
                completion_.bytes.load(std::memory_order_acquire));
        } else {
            if (completion_.error) {
                return Result<std::size_t>::failure(completion_.error);
            }
            return Result<std::size_t>::success(completion_.bytes);
        }
    }

private:
    using RequestStorage = std::conditional_t<
        kBatch, BatchRequestStorage, SingleRequestStorage>;
    using CompletionState = std::conditional_t<
        kBatch, BatchCompletionState, SingleCompletionState>;

    void record_error(int error) noexcept {
        static_assert(kBatch);
        if (error == 0) {
            return;
        }
        int expected = 0;
        (void)completion_.error.compare_exchange_strong(
            expected, error, std::memory_order_acq_rel);
    }

    [[nodiscard, gnu::always_inline]] int submit(
        const BatchRequest& request, void* context) noexcept {
        switch (operation_) {
        case Operation::read:
            return submit_read_data_from_devs(
                request.buffer, static_cast<std::int64_t>(request.length),
                static_cast<std::int64_t>(request.offset), &complete, context);
        case Operation::write:
            return submit_write_data_to_devs(
                request.buffer, static_cast<std::int64_t>(request.length),
                static_cast<std::int64_t>(request.offset), &complete, context);
        case Operation::flush:
            return submit_device_sync(&complete, context);
        }
        std::terminate();
    }

    static void complete(void* context, int error,
                         std::size_t bytes) noexcept {
        BatchDeviceAwaiter* awaiter;
        std::size_t expected;
        if constexpr (kBatch) {
            auto& completion = *static_cast<BatchCompletion*>(context);
            awaiter = completion.awaiter;
            expected = completion.expected;
        } else {
            awaiter = static_cast<BatchDeviceAwaiter*>(context);
            expected = awaiter->requests_.request.length;
        }
        if (error == 0 && bytes != expected) {
            error = EIO;
        }
        if constexpr (kBatch) {
            awaiter->record_error(error);
            if (error == 0) {
                awaiter->completion_.bytes.fetch_add(
                    bytes, std::memory_order_relaxed);
            }
            if (awaiter->completion_.outstanding.fetch_sub(
                    1, std::memory_order_acq_rel) != 1) {
                return;
            }
        } else {
            if (error != 0) {
                awaiter->completion_.error =
                    detail::errno_error<false>(error);
            }
            awaiter->completion_.bytes = bytes;
        }

        const auto previous = awaiter->submission_state_.exchange(
            SubmissionState::completed, std::memory_order_acq_rel);
        if (previous == SubmissionState::submitting) {
            return;
        }
        if (previous != SubmissionState::suspended) {
            std::terminate();
        }
        const auto continuation = std::exchange(
            awaiter->continuation_, std::coroutine_handle<>{});
        if (!continuation ||
            !awaiter->runtime_->schedule(continuation, awaiter->worker_)) {
            std::terminate();
        }
    }

    Runtime* runtime_;
    Operation operation_;
    RequestStorage requests_;
    std::size_t worker_{detail::no_worker};
    std::coroutine_handle<> continuation_{};
    CompletionState completion_;
    std::atomic<SubmissionState> submission_state_{
        SubmissionState::submitting};
};

template <Operation operation, typename Request>
Task<Result<std::size_t>> submit_batch(
    Runtime& runtime, std::span<const Request> requests) {
    static_assert(operation == Operation::read ||
                  operation == Operation::write);
    static_assert((operation == Operation::read &&
                   std::is_same_v<Request, BlockRead>) ||
                  (operation == Operation::write &&
                   std::is_same_v<Request, BlockWrite>));

    std::optional<BatchRequestList> native;
    std::array<BatchCompletion, kInlineBatchRequests> inline_completions{};
    std::vector<BatchCompletion> completions;
    try {
        native.emplace(requests.size());
        for (const auto& request : requests) {
            if constexpr (operation == Operation::read) {
                append_merged_request(*native, BatchRequest{
                    .offset = request.offset,
                    .buffer = request.destination.data(),
                    .length = request.destination.size(),
                });
            } else {
                append_merged_request(*native, BatchRequest{
                    .offset = request.offset,
                    .buffer = const_cast<std::byte*>(request.source.data()),
                    .length = request.source.size(),
                });
            }
        }
        if (native->size() > inline_completions.size()) {
            completions.resize(native->size());
        }
    } catch (const std::bad_alloc&) {
        co_return Result<std::size_t>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    }
    const auto completion_span = native->size() <= inline_completions.size()
        ? std::span<BatchCompletion>(inline_completions.data(), native->size())
        : std::span<BatchCompletion>(completions);
    co_return co_await BatchDeviceAwaiter<true>(
        runtime, operation, native->span(), completion_span);
}

} // namespace

Task<Result<std::size_t>> AsyncBlockDevice::read(
    std::uint64_t offset, std::span<std::byte> destination) const {
    repro_trace::Span trace(ORCHFS_TRACE_DEVICE_READ, 0,
                            destination.size());
    auto result = co_await BatchDeviceAwaiter<false>(
        *runtime_, Operation::read,
        BatchRequest{.offset = offset,
                     .buffer = destination.data(),
                     .length = destination.size()});
    trace.finish(result ? result.value() : 0,
                 result ? std::error_code{} : result.error());
    co_return result;
}

Task<Result<std::size_t>> AsyncBlockDevice::write(
    std::uint64_t offset, std::span<const std::byte> source) const {
    repro_trace::Span trace(ORCHFS_TRACE_DEVICE_WRITE, 0, source.size());
    auto result = co_await BatchDeviceAwaiter<false>(
        *runtime_, Operation::write,
        BatchRequest{.offset = offset,
                     .buffer = const_cast<std::byte*>(source.data()),
                     .length = source.size()});
    trace.finish(result ? result.value() : 0,
                 result ? std::error_code{} : result.error());
    co_return result;
}

Task<Result<std::size_t>> AsyncBlockDevice::read_batch(
    std::span<const BlockRead> requests) const {
    std::uint64_t bytes = 0;
    for (const auto& request : requests) {
        bytes += request.destination.size();
    }
    repro_trace::Span trace(ORCHFS_TRACE_DEVICE_READ, 0, bytes,
                            static_cast<std::uint32_t>(requests.size()));
    auto result = co_await submit_batch<Operation::read>(*runtime_, requests);
    trace.finish(result ? result.value() : 0,
                 result ? std::error_code{} : result.error());
    co_return result;
}

Task<Result<std::size_t>> AsyncBlockDevice::write_batch(
    std::span<const BlockWrite> requests) const {
    std::uint64_t bytes = 0;
    for (const auto& request : requests) {
        bytes += request.source.size();
    }
    repro_trace::Span trace(ORCHFS_TRACE_DEVICE_WRITE, 0, bytes,
                            static_cast<std::uint32_t>(requests.size()));
    auto result = co_await submit_batch<Operation::write>(*runtime_, requests);
    trace.finish(result ? result.value() : 0,
                 result ? std::error_code{} : result.error());
    co_return result;
}

Task<Result<void>> AsyncBlockDevice::flush() const {
    repro_trace::Span trace(ORCHFS_TRACE_DEVICE_FLUSH);
    auto flushed = co_await BatchDeviceAwaiter<false>(
        *runtime_, Operation::flush, BatchRequest{});
    if (!flushed) {
        trace.finish(0, flushed.error());
        co_return Result<void>::failure(flushed.error());
    }
    trace.finish(0, 0);
    co_return Result<void>::success();
}

DeviceWriteDurability AsyncBlockDevice::write_durability() const noexcept {
    switch (orchfs_device_effective_write_durability()) {
    case ORCHFS_DEVICE_DURABILITY_COMPLETION:
        return DeviceWriteDurability::completion;
    case ORCHFS_DEVICE_DURABILITY_FUA:
        return DeviceWriteDurability::fua;
    case ORCHFS_DEVICE_DURABILITY_FLUSH:
        return DeviceWriteDurability::flush;
    default:
        // A running production KFS always has a resolved SPDK policy.  Keep an
        // unknown test/detached backend conservative.
        return DeviceWriteDurability::flush;
    }
}

} // namespace orchfs::async
