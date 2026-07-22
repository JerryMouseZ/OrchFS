#pragma once

#include "orchfs/async/task.hpp"

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <system_error>
#include <vector>

namespace orchfs::async {

struct RuntimeOptions {
    // Zero selects one worker for every CPU in cpu_set (or the current process
    // affinity mask when cpu_set is empty).
    std::size_t worker_count{0};
    std::vector<unsigned> cpu_set;
    std::size_t spin_before_park{256};
    // External block_on callers may briefly spin before entering the futex
    // wait. Zero keeps the general Runtime default fully blocking.
    std::size_t blocking_spin_count{0};
    // At most this many concurrent block_on callers spin. Additional callers
    // sleep immediately so deep queues do not steal CPUs from Runtime workers.
    std::size_t blocking_spin_limit{0};
    // Observe this many completed calls before enabling adaptive spinning.
    // A deeper queue seen during the warmup permanently selects the sleep path.
    std::size_t blocking_spin_warmup{64};
    std::function<void(std::error_code)> on_unobserved_error;
};

struct RuntimePollerStats {
    std::size_t active_pollers{};
    std::size_t generations{};
};

class Runtime final {
public:
    enum class PollState : std::uint8_t {
        idle,
        busy,
        progress,
    };

    using PollFunction = PollState (*)(void* context) noexcept;

    class PollRegistration final {
    public:
        struct State;

        PollRegistration() noexcept = default;
        PollRegistration(const PollRegistration&) = delete;
        PollRegistration& operator=(const PollRegistration&) = delete;
        PollRegistration(PollRegistration&& other) noexcept;
        PollRegistration& operator=(PollRegistration&& other) noexcept;
        ~PollRegistration();

        void reset() noexcept;
        [[nodiscard]] explicit operator bool() const noexcept {
            return static_cast<bool>(state_);
        }

    private:
        explicit PollRegistration(std::shared_ptr<State> state) noexcept
            : state_(std::move(state)) {}

        std::shared_ptr<State> state_;

        friend class Runtime;
    };

    [[nodiscard]] static Result<std::unique_ptr<Runtime>> create(
        RuntimeOptions options = {});

    Runtime(const Runtime&) = delete;
    Runtime& operator=(const Runtime&) = delete;
    Runtime(Runtime&&) = delete;
    Runtime& operator=(Runtime&&) = delete;
    ~Runtime();

    template <typename T>
    [[nodiscard]] Result<JoinHandle<T>> submit(Task<T>&& task) {
        if (!task.valid()) {
            return Result<JoinHandle<T>>::failure(Errc::invalid_task);
        }

        std::shared_ptr<detail::CompletionState<T>> state;
        try {
            state = std::make_shared<detail::CompletionState<T>>(
                this, unobserved_error_handler());
        } catch (const std::bad_alloc&) {
            return Result<JoinHandle<T>>::failure(
                std::make_error_code(std::errc::not_enough_memory));
        } catch (...) {
            std::terminate();
        }

        auto coroutine = task.release();
        coroutine.promise().attach_root(state.get());
        state->set_root(coroutine);

        auto submitted = submit_root(state, coroutine);
        if (!submitted) {
            return Result<JoinHandle<T>>::failure(submitted.error());
        }
        return Result<JoinHandle<T>>::success(JoinHandle<T>(std::move(state)));
    }

    // Submit a root and synchronously consume it without allocating a shared
    // completion or entering the global root registry.  This is the blocking
    // compatibility bridge used at external ABIs such as POSIX/LD_PRELOAD;
    // coroutine code should continue to use structured co_await or submit().
    template <typename T>
    [[nodiscard]] Result<T> block_on(Task<T>&& task) noexcept(
        std::is_nothrow_move_constructible_v<T>) {
        if (!task.valid()) {
            return Result<T>::failure(Errc::invalid_task);
        }
        if (current() != nullptr) {
            return Result<T>::failure(current() == this
                                          ? Errc::join_from_worker
                                          : Errc::wrong_runtime);
        }

        detail::CompletionState<T> state(this, {}, true);
        if (!state.begin_consume()) {
            std::terminate();
        }
        auto coroutine = task.release();
        coroutine.promise().attach_root(&state);
        state.set_root(coroutine);

        auto submitted = submit_blocking_root(coroutine);
        if (!submitted) {
            return Result<T>::failure(submitted.error());
        }
        const std::size_t spin_count = begin_blocking_wait();
        state.wait(spin_count);
        end_blocking_wait();
        return state.take();
    }

    // Stop accepting new roots. Already submitted roots and their completions
    // are drained before worker shutdown.
    void request_stop() noexcept;

    // join() is a blocking external-thread operation. It implicitly requests
    // stop so destruction cannot leave worker threads behind.
    [[nodiscard]] Result<void> join() noexcept;

    [[nodiscard]] std::size_t worker_count() const noexcept;
    [[nodiscard]] Result<unsigned> worker_cpu(std::size_t worker) const noexcept;
    [[nodiscard]] std::size_t owner_for(std::uint64_t key) const noexcept;
    [[nodiscard]] RuntimePollerStats poller_stats() const noexcept;

    // Wake a worker so an owner-local poller can drain work published through
    // an external MPSC queue. No coroutine is created or scheduled.
    [[nodiscard]] bool notify(std::size_t worker) noexcept;

    // Register a non-blocking driver on one worker. The callback is invoked
    // only by that worker and must report whether it has outstanding work so
    // the Runtime does not park while progress still requires active polling.
    // Destroying the registration prevents future callbacks and waits for an
    // already-running callback to return.
    [[nodiscard]] Result<PollRegistration> register_poller(
        std::size_t worker, PollFunction function, void* context) noexcept;

    [[nodiscard]] static Runtime* current() noexcept;
    [[nodiscard]] static std::size_t current_worker() noexcept;

    // Thread-safe transport/completion entry point. A specified worker is put
    // on its non-stealable owner FIFO; registered range-pin metadata rejects
    // an incompatible target and is restored on resume. This stays available
    // while submitted roots are draining. Once the stopped Runtime reaches a
    // fully drained state, admission closes atomically with the workers' final
    // exit decision and schedule() returns false (possibly before join()).
    [[nodiscard]] bool schedule(
        std::coroutine_handle<> coroutine,
        std::optional<std::size_t> worker = std::nullopt) noexcept;

    class ScheduleOnAwaiter {
    public:
        ScheduleOnAwaiter(Runtime& runtime, std::size_t worker) noexcept
            : runtime_(&runtime), worker_(worker) {}

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<> coroutine) noexcept;
        [[nodiscard]] Result<void> await_resume() const noexcept;

    private:
        Runtime* runtime_;
        std::size_t worker_;
        std::error_code error_;
    };

    [[nodiscard]] ScheduleOnAwaiter schedule_on(std::size_t worker) noexcept {
        return ScheduleOnAwaiter(*this, worker);
    }

    class YieldAwaiter {
    public:
        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<> coroutine) noexcept;
        [[nodiscard]] Result<void> await_resume() const noexcept;

    private:
        std::error_code error_;
    };

    [[nodiscard]] static YieldAwaiter yield() noexcept {
        return {};
    }

private:
    struct Impl;

    explicit Runtime(std::unique_ptr<Impl> impl) noexcept;

    [[nodiscard]] static Result<std::unique_ptr<Impl>> make_impl(
        RuntimeOptions options);
    [[nodiscard]] Result<void> start() noexcept;
    [[nodiscard]] Result<void> submit_root(
        std::shared_ptr<detail::CompletionStateBase> state,
        std::coroutine_handle<> coroutine) noexcept;
    [[nodiscard]] Result<void> submit_blocking_root(
        std::coroutine_handle<> coroutine) noexcept;
    [[nodiscard]] detail::UnobservedErrorHandler
    unobserved_error_handler() const;
    [[nodiscard]] std::size_t begin_blocking_wait() noexcept;
    void end_blocking_wait() noexcept;

    [[nodiscard]] bool schedule_internal(std::coroutine_handle<> coroutine,
                                         std::size_t worker,
                                         unsigned pin_depth,
                                         bool owner) noexcept;
    void register_pin(std::coroutine_handle<> coroutine,
                      detail::ResumeTarget target) noexcept;
    void transfer_pin(std::coroutine_handle<> coroutine,
                      std::coroutine_handle<> continuation) noexcept;
    void unregister_pin(std::coroutine_handle<> coroutine) noexcept;
    void on_root_completed(detail::CompletionStateBase* state) noexcept;
    void on_blocking_root_completed(
        detail::CompletionStateBase* state) noexcept;

    std::unique_ptr<Impl> impl_;

    friend void detail::schedule_resume(detail::ResumeTarget,
                                        std::coroutine_handle<>,
                                        bool) noexcept;
    friend void detail::root_completed(Runtime*,
                                       detail::CompletionStateBase*) noexcept;
    friend void detail::blocking_root_completed(
        Runtime*, detail::CompletionStateBase*) noexcept;
    friend void detail::pin_current(std::coroutine_handle<>) noexcept;
    friend void detail::register_pin(std::coroutine_handle<>,
                                     detail::ResumeTarget) noexcept;
    friend void detail::transfer_pin_to_continuation(
        std::coroutine_handle<>, std::coroutine_handle<>) noexcept;
    friend void detail::unpin_current(std::coroutine_handle<>) noexcept;
};

} // namespace orchfs::async
