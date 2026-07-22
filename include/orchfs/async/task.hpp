#pragma once

#include "orchfs/async/result.hpp"

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace orchfs::async {

class Runtime;

namespace detail {

inline constexpr std::size_t no_worker = std::numeric_limits<std::size_t>::max();

struct ResumeTarget {
    Runtime* runtime{nullptr};
    std::size_t worker{no_worker};
    unsigned pin_depth{0};
    bool owner{false};
};

[[nodiscard]] ResumeTarget current_resume_target() noexcept;
void schedule_resume(ResumeTarget target,
                     std::coroutine_handle<> coroutine,
                     bool owner) noexcept;
void prepare_symmetric_resume(ResumeTarget target) noexcept;
void pin_current(std::coroutine_handle<> coroutine) noexcept;
void register_pin(std::coroutine_handle<> coroutine,
                  ResumeTarget target) noexcept;
void transfer_pin_to_continuation(std::coroutine_handle<> coroutine,
                                  std::coroutine_handle<> continuation) noexcept;
void unpin_current(std::coroutine_handle<> coroutine) noexcept;
[[nodiscard]] void* allocate_coroutine_frame(std::size_t size);
void deallocate_coroutine_frame(void* frame) noexcept;

class CompletionStateBase;
void root_completed(Runtime* runtime, CompletionStateBase* state) noexcept;
void blocking_root_completed(Runtime* runtime,
                             CompletionStateBase* state) noexcept;

using UnobservedErrorHandler = std::function<void(std::error_code)>;

class CompletionStateBase {
public:
    CompletionStateBase(Runtime* runtime, UnobservedErrorHandler handler,
                        bool blocking = false)
        : runtime_(runtime), unobserved_error_(std::move(handler)),
          blocking_(blocking) {}

    CompletionStateBase(const CompletionStateBase&) = delete;
    CompletionStateBase& operator=(const CompletionStateBase&) = delete;

    virtual ~CompletionStateBase() {
        if (root_) {
            root_.destroy();
        }
    }

    [[nodiscard]] bool ready() const noexcept {
        return waiter_state_.load(std::memory_order_acquire) ==
               completed_sentinel();
    }

    [[nodiscard]] bool begin_consume() noexcept {
        bool expected = false;
        if (!consumed_.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            return false;
        }
        observed_.store(true, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool register_waiter(std::coroutine_handle<> waiter,
                                       ResumeTarget target) noexcept {
        if (!waiter) {
            return false;
        }
        waiter_ = waiter;
        waiter_target_ = target;
        void* expected = nullptr;
        if (waiter_state_.compare_exchange_strong(
                expected, waiter.address(), std::memory_order_release,
                std::memory_order_acquire)) {
            return true;
        }
        if (expected != completed_sentinel()) {
            // begin_consume() guarantees that only one waiter can register.
            std::terminate();
        }
        return false;
    }

    void wait(std::size_t spin_count = 0) noexcept {
        void* state = waiter_state_.load(std::memory_order_acquire);
        while (state != completed_sentinel() && spin_count-- != 0) {
#if defined(__x86_64__) || defined(__i386__)
            __builtin_ia32_pause();
#else
            std::atomic_signal_fence(std::memory_order_seq_cst);
#endif
            state = waiter_state_.load(std::memory_order_acquire);
        }
        while (state != completed_sentinel()) {
            waiter_state_.wait(state, std::memory_order_acquire);
            state = waiter_state_.load(std::memory_order_acquire);
        }
    }

    void set_root(std::coroutine_handle<> root) noexcept {
        root_ = root;
    }

    // A blocking root is owned by an external stack completion.  Its frame
    // cannot be destroyed from final_suspend(), and the external waiter must
    // not be released until the worker's resume() has returned.  Runtime calls
    // these two operations, in order, from its deferred-completion phase.
    void destroy_blocking_root() noexcept {
        if (!blocking_ || !root_) {
            std::terminate();
        }
        auto root = std::exchange(root_, {});
        root.destroy();
    }

    void publish_blocking_ready() noexcept {
        if (!blocking_) {
            std::terminate();
        }
        publish_waiter_ready();
    }

    [[nodiscard]] Runtime* runtime() const noexcept {
        return runtime_;
    }

protected:
    void publish_ready() noexcept {
        if (blocking_) {
            blocking_root_completed(runtime_, this);
            return;
        }
        publish_waiter_ready();
        root_completed(runtime_, this);
    }

    [[nodiscard]] bool observed() const noexcept {
        return observed_.load(std::memory_order_acquire);
    }

    void report_unobserved(std::error_code error) noexcept {
        if (!error || observed() || !unobserved_error_) {
            return;
        }
        try {
            unobserved_error_(error);
        } catch (...) {
            std::terminate();
        }
    }

private:
    void publish_waiter_ready() noexcept {
        void* waiter = waiter_state_.exchange(
            completed_sentinel(), std::memory_order_acq_rel);
        waiter_state_.notify_all();
        if (waiter != nullptr) {
            if (waiter == completed_sentinel() ||
                waiter_.address() != waiter) {
                std::terminate();
            }
            schedule_resume(waiter_target_, waiter_,
                            waiter_target_.pin_depth != 0);
        }
    }

    static void* completed_sentinel() noexcept {
        return reinterpret_cast<void*>(static_cast<std::uintptr_t>(1));
    }

    Runtime* runtime_;
    UnobservedErrorHandler unobserved_error_;
    bool blocking_{false};
    std::coroutine_handle<> root_{};
    std::atomic<bool> consumed_{false};
    std::atomic<bool> observed_{false};
    std::atomic<void*> waiter_state_{nullptr};
    std::coroutine_handle<> waiter_{};
    ResumeTarget waiter_target_{};
};

template <typename T>
class CompletionState final : public CompletionStateBase {
public:
    using CompletionStateBase::CompletionStateBase;

    ~CompletionState() override {
        if constexpr (is_result_v<T>) {
            if (value_ && !*value_) {
                report_unobserved(value_->error());
            }
        }
    }

    void complete(T value) noexcept(std::is_nothrow_move_constructible_v<T>) {
        value_.emplace(std::move(value));
        publish_ready();
    }

    [[nodiscard]] Result<T> take() noexcept(std::is_nothrow_move_constructible_v<T>) {
        if (!value_) {
            return Result<T>::failure(Errc::invalid_handle);
        }
        T value = std::move(*value_);
        value_.reset();
        return Result<T>::success(std::move(value));
    }

private:
    std::optional<T> value_;
};

template <>
class CompletionState<void> final : public CompletionStateBase {
public:
    using CompletionStateBase::CompletionStateBase;

    void complete() noexcept {
        publish_ready();
    }

    [[nodiscard]] Result<void> take() noexcept {
        return Result<void>::success();
    }
};

struct TaskFinalAwaiter {
    [[nodiscard]] bool await_ready() const noexcept {
        return false;
    }

    template <typename Promise>
    [[nodiscard]] std::coroutine_handle<> await_suspend(
        std::coroutine_handle<Promise> coroutine) const noexcept {
        auto& promise = coroutine.promise();
        if (promise.continuation_) {
            // Nested tasks resume symmetrically. Any range permits still held
            // by the child (including a movable permit returned to its parent)
            // therefore become pins of the continuation frame.
            transfer_pin_to_continuation(coroutine,
                                         promise.continuation_);
            return promise.continuation_;
        }
        // Range ownership may move through structured nested Tasks, but it
        // must not escape a submitted root through JoinHandle: no coroutine
        // frame would remain to own the worker pin or perform direct handoff.
        if (current_resume_target().pin_depth != 0) {
            std::terminate();
        }
        promise.publish_root();
        return std::noop_coroutine();
    }

    void await_resume() const noexcept {}
};

} // namespace detail

template <typename T = void>
class [[nodiscard]] Task;

template <typename T>
class [[nodiscard]] JoinHandle {
public:
    JoinHandle() = default;
    JoinHandle(const JoinHandle&) = delete;
    JoinHandle& operator=(const JoinHandle&) = delete;
    JoinHandle(JoinHandle&&) noexcept = default;
    JoinHandle& operator=(JoinHandle&&) noexcept = default;
    ~JoinHandle() = default;

    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(state_);
    }

    [[nodiscard]] bool ready() const noexcept {
        return state_ && state_->ready();
    }

    [[nodiscard]] Result<T> join() && noexcept(
        std::is_nothrow_move_constructible_v<T>) {
        auto state = std::move(state_);
        if (!state) {
            return Result<T>::failure(Errc::invalid_handle);
        }
        if (detail::current_resume_target().runtime != nullptr) {
            return Result<T>::failure(Errc::join_from_worker);
        }
        if (!state->begin_consume()) {
            return Result<T>::failure(Errc::already_consumed);
        }
        state->wait();
        return state->take();
    }

    class Awaiter {
    public:
        explicit Awaiter(std::shared_ptr<detail::CompletionState<T>> state) noexcept
            : state_(std::move(state)) {
            if (!state_) {
                error_ = make_error_code(Errc::invalid_handle);
            } else if (!state_->begin_consume()) {
                error_ = make_error_code(Errc::already_consumed);
            }
        }

        [[nodiscard]] bool await_ready() const noexcept {
            return error_ || state_->ready();
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<> waiter) noexcept {
            return state_->register_waiter(waiter,
                                           detail::current_resume_target());
        }

        [[nodiscard]] Result<T> await_resume() noexcept(
            std::is_nothrow_move_constructible_v<T>) {
            if (error_) {
                return Result<T>::failure(error_);
            }
            return state_->take();
        }

    private:
        std::shared_ptr<detail::CompletionState<T>> state_;
        std::error_code error_;
    };

    [[nodiscard]] Awaiter operator co_await() && noexcept {
        return Awaiter(std::move(state_));
    }

    Awaiter operator co_await() & = delete;

private:
    explicit JoinHandle(std::shared_ptr<detail::CompletionState<T>> state) noexcept
        : state_(std::move(state)) {}

    std::shared_ptr<detail::CompletionState<T>> state_;

    friend class Runtime;
};

template <>
class [[nodiscard]] JoinHandle<void> {
public:
    JoinHandle() = default;
    JoinHandle(const JoinHandle&) = delete;
    JoinHandle& operator=(const JoinHandle&) = delete;
    JoinHandle(JoinHandle&&) noexcept = default;
    JoinHandle& operator=(JoinHandle&&) noexcept = default;
    ~JoinHandle() = default;

    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(state_);
    }

    [[nodiscard]] bool ready() const noexcept {
        return state_ && state_->ready();
    }

    [[nodiscard]] Result<void> join() && noexcept {
        auto state = std::move(state_);
        if (!state) {
            return Result<void>::failure(Errc::invalid_handle);
        }
        if (detail::current_resume_target().runtime != nullptr) {
            return Result<void>::failure(Errc::join_from_worker);
        }
        if (!state->begin_consume()) {
            return Result<void>::failure(Errc::already_consumed);
        }
        state->wait();
        return state->take();
    }

    class Awaiter {
    public:
        explicit Awaiter(std::shared_ptr<detail::CompletionState<void>> state) noexcept
            : state_(std::move(state)) {
            if (!state_) {
                error_ = make_error_code(Errc::invalid_handle);
            } else if (!state_->begin_consume()) {
                error_ = make_error_code(Errc::already_consumed);
            }
        }

        [[nodiscard]] bool await_ready() const noexcept {
            return error_ || state_->ready();
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<> waiter) noexcept {
            return state_->register_waiter(waiter,
                                           detail::current_resume_target());
        }

        [[nodiscard]] Result<void> await_resume() noexcept {
            if (error_) {
                return Result<void>::failure(error_);
            }
            return state_->take();
        }

    private:
        std::shared_ptr<detail::CompletionState<void>> state_;
        std::error_code error_;
    };

    [[nodiscard]] Awaiter operator co_await() && noexcept {
        return Awaiter(std::move(state_));
    }

    Awaiter operator co_await() & = delete;

private:
    explicit JoinHandle(std::shared_ptr<detail::CompletionState<void>> state) noexcept
        : state_(std::move(state)) {}

    std::shared_ptr<detail::CompletionState<void>> state_;

    friend class Runtime;
};

template <typename T>
class [[nodiscard]] Task {
public:
    struct promise_type {
        static void* operator new(std::size_t size) {
            return detail::allocate_coroutine_frame(size);
        }

        static void operator delete(void* frame) noexcept {
            detail::deallocate_coroutine_frame(frame);
        }

        static void operator delete(void* frame, std::size_t) noexcept {
            detail::deallocate_coroutine_frame(frame);
        }

        [[nodiscard]] Task get_return_object() noexcept {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        [[nodiscard]] std::suspend_always initial_suspend() const noexcept {
            return {};
        }

        [[nodiscard]] detail::TaskFinalAwaiter final_suspend() const noexcept {
            return {};
        }

        template <typename U>
            requires std::constructible_from<T, U&&>
        void return_value(U&& value) noexcept(
            std::is_nothrow_constructible_v<T, U&&>) {
            value_.emplace(std::forward<U>(value));
        }

        [[noreturn]] void unhandled_exception() const noexcept {
            std::terminate();
        }

    private:
        void attach_root(detail::CompletionState<T>* completion) noexcept {
            completion_ = completion;
        }

        void publish_root() noexcept(std::is_nothrow_move_constructible_v<T>) {
            if (!completion_ || !value_) {
                std::terminate();
            }
            completion_->complete(std::move(*value_));
            value_.reset();
        }

        std::optional<T> value_;
        std::coroutine_handle<> continuation_{};
        detail::CompletionState<T>* completion_{nullptr};
        friend class Task;
        friend class Runtime;
        friend struct detail::TaskFinalAwaiter;
    };

    Task() = default;
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : coroutine_(std::exchange(other.coroutine_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (coroutine_) {
                coroutine_.destroy();
            }
            coroutine_ = std::exchange(other.coroutine_, {});
        }
        return *this;
    }

    ~Task() {
        if (coroutine_) {
            coroutine_.destroy();
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(coroutine_);
    }

    class Awaiter {
    public:
        explicit Awaiter(std::coroutine_handle<promise_type> coroutine) noexcept
            : coroutine_(coroutine) {}

        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;

        Awaiter(Awaiter&& other) noexcept
            : coroutine_(std::exchange(other.coroutine_, {})) {}

        ~Awaiter() {
            if (coroutine_) {
                coroutine_.destroy();
            }
        }

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            coroutine_.promise().continuation_ = continuation;
            const auto target = detail::current_resume_target();
            if (target.pin_depth != 0) {
                detail::register_pin(coroutine_, target);
            }
            return coroutine_;
        }

        [[nodiscard]] T await_resume() noexcept(
            std::is_nothrow_move_constructible_v<T>) {
            if (!coroutine_.promise().value_) {
                std::terminate();
            }
            T value = std::move(*coroutine_.promise().value_);
            coroutine_.destroy();
            coroutine_ = {};
            return value;
        }

    private:
        std::coroutine_handle<promise_type> coroutine_{};
    };

    [[nodiscard]] Awaiter operator co_await() && noexcept {
        return Awaiter(std::exchange(coroutine_, {}));
    }

    Awaiter operator co_await() & = delete;

private:
    explicit Task(std::coroutine_handle<promise_type> coroutine) noexcept
        : coroutine_(coroutine) {}

    [[nodiscard]] std::coroutine_handle<promise_type> release() noexcept {
        return std::exchange(coroutine_, {});
    }

    std::coroutine_handle<promise_type> coroutine_{};

    friend class Runtime;
};

template <>
class [[nodiscard]] Task<void> {
public:
    struct promise_type {
        static void* operator new(std::size_t size) {
            return detail::allocate_coroutine_frame(size);
        }

        static void operator delete(void* frame) noexcept {
            detail::deallocate_coroutine_frame(frame);
        }

        static void operator delete(void* frame, std::size_t) noexcept {
            detail::deallocate_coroutine_frame(frame);
        }

        [[nodiscard]] Task get_return_object() noexcept {
            return Task(std::coroutine_handle<promise_type>::from_promise(*this));
        }

        [[nodiscard]] std::suspend_always initial_suspend() const noexcept {
            return {};
        }

        [[nodiscard]] detail::TaskFinalAwaiter final_suspend() const noexcept {
            return {};
        }

        void return_void() noexcept {}

        [[noreturn]] void unhandled_exception() const noexcept {
            std::terminate();
        }

    private:
        void attach_root(detail::CompletionState<void>* completion) noexcept {
            completion_ = completion;
        }

        void publish_root() noexcept {
            if (!completion_) {
                std::terminate();
            }
            completion_->complete();
        }

        std::coroutine_handle<> continuation_{};
        detail::CompletionState<void>* completion_{nullptr};
        friend class Task;
        friend class Runtime;
        friend struct detail::TaskFinalAwaiter;
    };

    Task() = default;
    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : coroutine_(std::exchange(other.coroutine_, {})) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            if (coroutine_) {
                coroutine_.destroy();
            }
            coroutine_ = std::exchange(other.coroutine_, {});
        }
        return *this;
    }

    ~Task() {
        if (coroutine_) {
            coroutine_.destroy();
        }
    }

    [[nodiscard]] bool valid() const noexcept {
        return static_cast<bool>(coroutine_);
    }

    class Awaiter {
    public:
        explicit Awaiter(std::coroutine_handle<promise_type> coroutine) noexcept
            : coroutine_(coroutine) {}

        Awaiter(const Awaiter&) = delete;
        Awaiter& operator=(const Awaiter&) = delete;

        Awaiter(Awaiter&& other) noexcept
            : coroutine_(std::exchange(other.coroutine_, {})) {}

        ~Awaiter() {
            if (coroutine_) {
                coroutine_.destroy();
            }
        }

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> continuation) noexcept {
            coroutine_.promise().continuation_ = continuation;
            const auto target = detail::current_resume_target();
            if (target.pin_depth != 0) {
                detail::register_pin(coroutine_, target);
            }
            return coroutine_;
        }

        void await_resume() noexcept {
            coroutine_.destroy();
            coroutine_ = {};
        }

    private:
        std::coroutine_handle<promise_type> coroutine_{};
    };

    [[nodiscard]] Awaiter operator co_await() && noexcept {
        return Awaiter(std::exchange(coroutine_, {}));
    }

    Awaiter operator co_await() & = delete;

private:
    explicit Task(std::coroutine_handle<promise_type> coroutine) noexcept
        : coroutine_(coroutine) {}

    [[nodiscard]] std::coroutine_handle<promise_type> release() noexcept {
        return std::exchange(coroutine_, {});
    }

    std::coroutine_handle<promise_type> coroutine_{};

    friend class Runtime;
};

} // namespace orchfs::async
