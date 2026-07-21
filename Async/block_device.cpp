#include "orchfs/async/block_device.hpp"

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

class DeviceAwaiter final {
public:
    enum class SubmissionState : std::uint8_t {
        submitting,
        suspended,
        completed,
    };

    DeviceAwaiter(Runtime& runtime, Operation operation, std::uint64_t offset,
                  void* buffer, std::size_t length) noexcept
        : runtime_(&runtime), operation_(operation), offset_(offset),
          buffer_(buffer), length_(length) {}

    [[nodiscard]] bool await_ready() const noexcept { return false; }

    [[nodiscard]] bool await_suspend(
        std::coroutine_handle<> continuation) noexcept {
        if (Runtime::current() != runtime_ ||
            Runtime::current_worker() == detail::no_worker) {
            error_ = make_error_code(Errc::not_in_runtime);
            return false;
        }
        constexpr auto max_offset = static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());
        if (offset_ > max_offset ||
            length_ > static_cast<std::size_t>(max_offset - offset_)) {
            error_ = std::make_error_code(std::errc::value_too_large);
            return false;
        }

        continuation_ = continuation;
        worker_ = Runtime::current_worker();
        int error = 0;
        switch (operation_) {
        case Operation::read:
            error = submit_read_data_from_devs(
                buffer_, static_cast<std::int64_t>(length_),
                static_cast<std::int64_t>(offset_), &complete, this);
            break;
        case Operation::write:
            error = submit_write_data_to_devs(
                buffer_, static_cast<std::int64_t>(length_),
                static_cast<std::int64_t>(offset_), &complete, this);
            break;
        case Operation::flush:
            error = submit_device_sync(&complete, this);
            break;
        }
        if (error != 0) {
            error_ = std::error_code(error, std::generic_category());
            continuation_ = {};
            return false;
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
        if (error_) {
            return Result<std::size_t>::failure(error_);
        }
        return Result<std::size_t>::success(bytes_);
    }

private:
    static void complete(void* context, int error_number,
                         std::size_t bytes) noexcept {
        auto& awaiter = *static_cast<DeviceAwaiter*>(context);
        if (error_number == 0 && bytes != awaiter.length_) {
            error_number = EIO;
        }
        if (error_number != 0) {
            awaiter.error_ =
                std::error_code(error_number, std::generic_category());
        }
        awaiter.bytes_ = bytes;
        const auto previous = awaiter.submission_state_.exchange(
            SubmissionState::completed, std::memory_order_acq_rel);
        if (previous == SubmissionState::submitting) {
            return;
        }
        if (previous != SubmissionState::suspended) {
            std::terminate();
        }
        const auto continuation =
            std::exchange(awaiter.continuation_, std::coroutine_handle<>{});
        if (!continuation ||
            !awaiter.runtime_->schedule(continuation, awaiter.worker_)) {
            std::terminate();
        }
    }

    Runtime* runtime_;
    Operation operation_;
    std::uint64_t offset_{};
    void* buffer_{};
    std::size_t length_{};
    std::size_t worker_{detail::no_worker};
    std::coroutine_handle<> continuation_{};
    std::error_code error_;
    std::size_t bytes_{};
    std::atomic<SubmissionState> submission_state_{
        SubmissionState::submitting};
};

class BatchDeviceAwaiter;

struct BatchCompletion {
    BatchDeviceAwaiter* awaiter{};
    std::size_t expected{};
};

class BatchDeviceAwaiter final {
public:
    enum class SubmissionState : std::uint8_t {
        submitting,
        suspended,
        completed,
    };

    BatchDeviceAwaiter(Runtime& runtime, Operation operation,
                       std::span<const BatchRequest> requests,
                       std::span<BatchCompletion> completions) noexcept
        : runtime_(&runtime), operation_(operation), requests_(requests),
          completions_(completions) {}

    [[nodiscard]] bool await_ready() const noexcept {
        return requests_.empty();
    }

    [[nodiscard]] bool await_suspend(
        std::coroutine_handle<> continuation) noexcept {
        if (Runtime::current() != runtime_ ||
            Runtime::current_worker() == detail::no_worker) {
            error_.store(make_error_code(Errc::not_in_runtime).value(),
                         std::memory_order_relaxed);
            return false;
        }
        continuation_ = continuation;
        worker_ = Runtime::current_worker();
        constexpr auto max_offset = static_cast<std::uint64_t>(
            std::numeric_limits<std::int64_t>::max());

        if (completions_.size() != requests_.size()) {
            record_error(EINVAL);
            continuation_ = {};
            return false;
        }

        for (std::size_t index = 0; index < requests_.size(); ++index) {
            const auto& request = requests_[index];
            if ((request.buffer == nullptr && request.length != 0) ||
                request.offset > max_offset ||
                request.length >
                    static_cast<std::size_t>(max_offset - request.offset)) {
                record_error(EINVAL);
                break;
            }
            completions_[index] = BatchCompletion{
                .awaiter = this,
                .expected = request.length,
            };
            outstanding_.fetch_add(1, std::memory_order_relaxed);
            const int submitted = operation_ == Operation::read
                ? submit_read_data_from_devs(
                      request.buffer, static_cast<std::int64_t>(request.length),
                      static_cast<std::int64_t>(request.offset), &complete,
                      &completions_[index])
                : submit_write_data_to_devs(
                      request.buffer, static_cast<std::int64_t>(request.length),
                      static_cast<std::int64_t>(request.offset), &complete,
                      &completions_[index]);
            if (submitted != 0) {
                outstanding_.fetch_sub(1, std::memory_order_relaxed);
                record_error(submitted);
                break;
            }
        }

        // Drop the submission sentinel after every request that reached the
        // device has installed its callback.
        if (outstanding_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            submission_state_.store(SubmissionState::completed,
                                    std::memory_order_release);
            continuation_ = {};
            return false;
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
        const int error = error_.load(std::memory_order_acquire);
        if (error != 0) {
            return Result<std::size_t>::failure(
                std::error_code(error, std::generic_category()));
        }
        return Result<std::size_t>::success(
            bytes_.load(std::memory_order_acquire));
    }

private:
    void record_error(int error) noexcept {
        if (error == 0) {
            return;
        }
        int expected = 0;
        (void)error_.compare_exchange_strong(
            expected, error, std::memory_order_acq_rel);
    }

    static void complete(void* context, int error,
                         std::size_t bytes) noexcept {
        auto& completion = *static_cast<BatchCompletion*>(context);
        auto& awaiter = *completion.awaiter;
        if (error == 0 && bytes != completion.expected) {
            error = EIO;
        }
        awaiter.record_error(error);
        if (error == 0) {
            awaiter.bytes_.fetch_add(bytes, std::memory_order_relaxed);
        }
        if (awaiter.outstanding_.fetch_sub(1, std::memory_order_acq_rel) != 1) {
            return;
        }
        const auto previous = awaiter.submission_state_.exchange(
            SubmissionState::completed, std::memory_order_acq_rel);
        if (previous == SubmissionState::submitting) {
            return;
        }
        if (previous != SubmissionState::suspended) {
            std::terminate();
        }
        const auto continuation = std::exchange(
            awaiter.continuation_, std::coroutine_handle<>{});
        if (!continuation ||
            !awaiter.runtime_->schedule(continuation, awaiter.worker_)) {
            std::terminate();
        }
    }

    Runtime* runtime_;
    Operation operation_;
    std::span<const BatchRequest> requests_;
    std::span<BatchCompletion> completions_;
    std::size_t worker_{detail::no_worker};
    std::coroutine_handle<> continuation_{};
    std::atomic<std::size_t> outstanding_{1};
    std::atomic<std::size_t> bytes_{0};
    std::atomic<int> error_{0};
    std::atomic<SubmissionState> submission_state_{
        SubmissionState::submitting};
};

} // namespace

Task<Result<std::size_t>> AsyncBlockDevice::read(
    std::uint64_t offset, std::span<std::byte> destination) const {
    co_return co_await DeviceAwaiter(
        *runtime_, Operation::read, offset, destination.data(),
        destination.size());
}

Task<Result<std::size_t>> AsyncBlockDevice::write(
    std::uint64_t offset, std::span<const std::byte> source) const {
    co_return co_await DeviceAwaiter(
        *runtime_, Operation::write, offset, const_cast<std::byte*>(source.data()),
        source.size());
}

Task<Result<std::size_t>> AsyncBlockDevice::read_batch(
    std::span<const BlockRead> requests) const {
    std::optional<BatchRequestList> native;
    std::array<BatchCompletion, kInlineBatchRequests> inline_completions{};
    std::vector<BatchCompletion> completions;
    try {
        native.emplace(requests.size());
        for (const auto& request : requests) {
            append_merged_request(*native, BatchRequest{
                .offset = request.offset,
                .buffer = request.destination.data(),
                .length = request.destination.size(),
            });
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
    co_return co_await BatchDeviceAwaiter(
        *runtime_, Operation::read, native->span(), completion_span);
}

Task<Result<std::size_t>> AsyncBlockDevice::write_batch(
    std::span<const BlockWrite> requests) const {
    std::optional<BatchRequestList> native;
    std::array<BatchCompletion, kInlineBatchRequests> inline_completions{};
    std::vector<BatchCompletion> completions;
    try {
        native.emplace(requests.size());
        for (const auto& request : requests) {
            append_merged_request(*native, BatchRequest{
                .offset = request.offset,
                .buffer = const_cast<std::byte*>(request.source.data()),
                .length = request.source.size(),
            });
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
    co_return co_await BatchDeviceAwaiter(
        *runtime_, Operation::write, native->span(), completion_span);
}

Task<Result<void>> AsyncBlockDevice::flush() const {
    auto flushed = co_await DeviceAwaiter(*runtime_, Operation::flush, 0,
                                          nullptr, 0);
    if (!flushed) {
        co_return Result<void>::failure(flushed.error());
    }
    co_return Result<void>::success();
}

} // namespace orchfs::async
