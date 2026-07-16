#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <new>
#include <pthread.h>
#include <sched.h>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <unistd.h>
#include <utility>

namespace orchfs::async {
namespace {

std::atomic<Runtime*> active_runtime{nullptr};
std::atomic<pid_t> active_runtime_pid{0};

thread_local Runtime* tls_runtime = nullptr;
thread_local std::size_t tls_worker = detail::no_worker;
thread_local unsigned tls_pin_depth = 0;
thread_local bool tls_owner = false;

struct WorkItem {
    std::coroutine_handle<> coroutine{};
    unsigned pin_depth{0};
    bool owner{false};
};

[[nodiscard]] std::vector<unsigned> current_affinity() {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    if (::sched_getaffinity(0, sizeof(mask), &mask) != 0) {
        return {};
    }

    std::vector<unsigned> cpus;
    for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
        if (CPU_ISSET(cpu, &mask)) {
            cpus.push_back(cpu);
        }
    }
    return cpus;
}

[[nodiscard]] std::error_code pthread_error(int error) noexcept {
    return {error, std::generic_category()};
}

} // namespace

struct Runtime::Impl {
    struct Worker {
        std::mutex queue_mutex;
        std::deque<WorkItem> owner_fifo;
        std::deque<WorkItem> stealable;

        std::mutex inbox_mutex;
        std::deque<WorkItem> inbox;

        std::thread thread;
        bool take_owner_next{true};
        std::deque<detail::CompletionStateBase*> deferred_completed;
    };

    explicit Impl(RuntimeOptions runtime_options)
        : options(std::move(runtime_options)) {}

    Runtime* runtime{nullptr};
    RuntimeOptions options;
    std::vector<unsigned> cpus;
    std::vector<std::unique_ptr<Worker>> workers;

    std::atomic<bool> accepting{true};
    std::atomic<bool> stop_requested{false};
    std::atomic<bool> running{false};
    std::atomic<bool> joined{false};
    std::atomic<std::size_t> pending_roots{0};
    std::atomic<std::size_t> scheduled_items{0};
    std::atomic<std::size_t> round_robin{0};
    std::atomic<std::uint64_t> wake_epoch{0};

    // Admission and the final worker-exit decision share this mutex.  Without
    // that linearization point, a worker can observe empty queues and exit
    // while an external completion concurrently enqueues work after the
    // observation but before Runtime::join() publishes joined=true.
    std::shared_mutex schedule_mutex;
    bool scheduling_closed{false};

    std::mutex roots_mutex;
    std::unordered_map<detail::CompletionStateBase*,
                       std::shared_ptr<detail::CompletionStateBase>> roots;
    std::deque<detail::CompletionStateBase*> completed_roots;

    std::mutex pins_mutex;
    std::unordered_map<void*, detail::ResumeTarget> pinned_coroutines;

    std::mutex wake_mutex;
    std::condition_variable wake_cv;

    std::mutex start_mutex;
    std::condition_variable start_cv;
    bool start_gate{false};
    bool abort_start{false};

    std::mutex join_mutex;

    void wake_one() noexcept {
        wake_epoch.fetch_add(1, std::memory_order_release);
        wake_cv.notify_one();
    }

    void wake_all() noexcept {
        wake_epoch.fetch_add(1, std::memory_order_release);
        wake_cv.notify_all();
    }

    void drain_inbox(std::size_t worker_index) noexcept {
        auto& worker = *workers[worker_index];
        std::deque<WorkItem> incoming;
        {
            std::lock_guard lock(worker.inbox_mutex);
            incoming.swap(worker.inbox);
        }
        if (incoming.empty()) {
            return;
        }

        std::lock_guard lock(worker.queue_mutex);
        while (!incoming.empty()) {
            WorkItem item = std::move(incoming.front());
            incoming.pop_front();
            if (item.owner) {
                worker.owner_fifo.push_back(std::move(item));
            } else {
                worker.stealable.push_back(std::move(item));
            }
        }
    }

    [[nodiscard]] bool pop_local(std::size_t worker_index,
                                 WorkItem& result) noexcept {
        drain_inbox(worker_index);
        auto& worker = *workers[worker_index];
        std::lock_guard lock(worker.queue_mutex);

        const bool has_owner = !worker.owner_fifo.empty();
        const bool has_stealable = !worker.stealable.empty();
        if (!has_owner && !has_stealable) {
            return false;
        }

        if (has_owner && has_stealable) {
            if (worker.take_owner_next) {
                result = std::move(worker.owner_fifo.front());
                worker.owner_fifo.pop_front();
            } else {
                result = std::move(worker.stealable.front());
                worker.stealable.pop_front();
            }
            worker.take_owner_next = !worker.take_owner_next;
            return true;
        }

        if (has_owner) {
            result = std::move(worker.owner_fifo.front());
            worker.owner_fifo.pop_front();
        } else {
            result = std::move(worker.stealable.front());
            worker.stealable.pop_front();
        }
        return true;
    }

    [[nodiscard]] bool steal(std::size_t thief, WorkItem& result) noexcept {
        const std::size_t count = workers.size();
        for (std::size_t step = 1; step < count; ++step) {
            const std::size_t victim_index = (thief + step) % count;
            auto& victim = *workers[victim_index];
            std::lock_guard lock(victim.queue_mutex);
            if (victim.stealable.empty()) {
                continue;
            }
            result = std::move(victim.stealable.back());
            victim.stealable.pop_back();
            result.owner = false;
            return true;
        }
        return false;
    }

    [[nodiscard]] bool queues_empty() noexcept {
        for (auto& worker_ptr : workers) {
            auto& worker = *worker_ptr;
            {
                std::lock_guard lock(worker.inbox_mutex);
                if (!worker.inbox.empty()) {
                    return false;
                }
            }
            {
                std::lock_guard lock(worker.queue_mutex);
                if (!worker.owner_fifo.empty() || !worker.stealable.empty()) {
                    return false;
                }
            }
        }
        return true;
    }

    [[nodiscard]] bool close_scheduling_if_drained() noexcept {
        std::unique_lock admission_lock(schedule_mutex);
        if (scheduling_closed) {
            return true;
        }
        if (!stop_requested.load(std::memory_order_acquire) ||
            pending_roots.load(std::memory_order_acquire) != 0 ||
            scheduled_items.load(std::memory_order_acquire) != 0 ||
            !queues_empty()) {
            return false;
        }
        scheduling_closed = true;
        return true;
    }

    void reap_completed() noexcept {
        std::deque<std::shared_ptr<detail::CompletionStateBase>> garbage;
        {
            std::lock_guard lock(roots_mutex);
            while (!completed_roots.empty()) {
                auto* completed = completed_roots.front();
                completed_roots.pop_front();
                auto found = roots.find(completed);
                if (found != roots.end()) {
                    garbage.push_back(std::move(found->second));
                    roots.erase(found);
                }
            }
        }
        // Coroutine frames are destroyed here, after resume() has returned.
        garbage.clear();
    }

    void publish_deferred_completions(std::size_t worker_index) noexcept {
        auto& deferred = workers[worker_index]->deferred_completed;
        if (deferred.empty()) {
            return;
        }
        std::lock_guard lock(roots_mutex);
        while (!deferred.empty()) {
            completed_roots.push_back(deferred.front());
            deferred.pop_front();
        }
    }

    void worker_loop(std::size_t worker_index) noexcept {
        {
            std::unique_lock lock(start_mutex);
            start_cv.wait(lock, [this] { return start_gate; });
            if (abort_start) {
                return;
            }
        }

        tls_runtime = runtime;
        tls_worker = worker_index;
        tls_pin_depth = 0;
        tls_owner = false;

        std::size_t spins = 0;
        std::uint64_t observed_epoch =
            wake_epoch.load(std::memory_order_acquire);

        for (;;) {
            WorkItem item;
            if (pop_local(worker_index, item) || steal(worker_index, item)) {
                spins = 0;
                tls_worker = worker_index;
                tls_pin_depth = item.pin_depth;
                tls_owner = item.owner;
                item.coroutine.resume();
                tls_worker = worker_index;
                tls_pin_depth = 0;
                tls_owner = false;
                publish_deferred_completions(worker_index);
                scheduled_items.fetch_sub(1, std::memory_order_acq_rel);
                reap_completed();
                continue;
            }

            reap_completed();
            if (close_scheduling_if_drained()) {
                break;
            }

            if (spins++ < options.spin_before_park) {
                std::this_thread::yield();
                continue;
            }

            spins = 0;
            std::unique_lock lock(wake_mutex);
            // request_stop() advances wake_epoch before notifying.  Do not use
            // stop_requested itself as a wake predicate: a stopped Runtime may
            // still have roots suspended on external I/O, and a permanently
            // true predicate would make every idle worker busy-spin while the
            // completion is outstanding.
            wake_cv.wait_for(lock, std::chrono::milliseconds(10), [&] {
                return wake_epoch.load(std::memory_order_acquire) !=
                       observed_epoch;
            });
            observed_epoch = wake_epoch.load(std::memory_order_acquire);
        }

        tls_pin_depth = 0;
        tls_owner = false;
        tls_worker = detail::no_worker;
        tls_runtime = nullptr;
    }
};

Runtime::Runtime(std::unique_ptr<Impl> impl) noexcept
    : impl_(std::move(impl)) {
    impl_->runtime = this;
}

Result<std::unique_ptr<Runtime::Impl>> Runtime::make_impl(
    RuntimeOptions options) {
    try {
        std::vector<unsigned> available = current_affinity();
        if (available.empty()) {
            return Result<std::unique_ptr<Impl>>::failure(
                errno ? std::error_code(errno, std::generic_category())
                      : std::make_error_code(std::errc::no_such_device));
        }

        if (options.cpu_set.empty()) {
            options.cpu_set = available;
        } else {
            for (unsigned cpu : options.cpu_set) {
                if (std::find(available.begin(), available.end(), cpu) ==
                    available.end()) {
                    return Result<std::unique_ptr<Impl>>::failure(
                        std::make_error_code(std::errc::invalid_argument));
                }
            }
        }

        if (options.worker_count == 0) {
            options.worker_count = options.cpu_set.size();
        }
        if (options.worker_count == 0 ||
            options.worker_count > options.cpu_set.size()) {
            return Result<std::unique_ptr<Impl>>::failure(
                std::make_error_code(std::errc::invalid_argument));
        }
        options.cpu_set.resize(options.worker_count);

        auto impl = std::make_unique<Impl>(std::move(options));
        impl->cpus = impl->options.cpu_set;
        impl->workers.reserve(impl->options.worker_count);
        for (std::size_t i = 0; i < impl->options.worker_count; ++i) {
            impl->workers.push_back(std::make_unique<Impl::Worker>());
        }
        return Result<std::unique_ptr<Impl>>::success(std::move(impl));
    } catch (const std::bad_alloc&) {
        return Result<std::unique_ptr<Impl>>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        std::terminate();
    }
}

Result<std::unique_ptr<Runtime>> Runtime::create(RuntimeOptions options) {
    const pid_t process = ::getpid();
    for (;;) {
        pid_t recorded_process =
            active_runtime_pid.load(std::memory_order_acquire);
        if (recorded_process == process) {
            break;
        }
        if (recorded_process == -process) {
            // Another thread in this process is replacing fork-inherited
            // singleton state. Wait until both atomics describe the new
            // process; otherwise a concurrent creator could observe the
            // parent's pointer or clear a Runtime that was just installed.
            std::this_thread::yield();
            continue;
        }
        if (active_runtime_pid.compare_exchange_weak(
                recorded_process, -process, std::memory_order_acq_rel,
                std::memory_order_acquire)) {
            // fork() preserves memory but not worker threads. The child must
            // never treat the parent's Runtime pointer as live. The inherited
            // object is deliberately abandoned and its resources are reclaimed
            // by process exit.
            active_runtime.store(nullptr, std::memory_order_release);
            active_runtime_pid.store(process, std::memory_order_release);
            break;
        }
    }
    if (active_runtime.load(std::memory_order_acquire) != nullptr) {
        return Result<std::unique_ptr<Runtime>>::failure(
            Errc::runtime_already_exists);
    }

    auto made_impl = make_impl(std::move(options));
    if (!made_impl) {
        return Result<std::unique_ptr<Runtime>>::failure(made_impl.error());
    }

    std::unique_ptr<Runtime> runtime;
    try {
        runtime.reset(new Runtime(std::move(made_impl).value()));
    } catch (const std::bad_alloc&) {
        return Result<std::unique_ptr<Runtime>>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        std::terminate();
    }

    Runtime* expected = nullptr;
    if (!active_runtime.compare_exchange_strong(
            expected, runtime.get(), std::memory_order_acq_rel)) {
        return Result<std::unique_ptr<Runtime>>::failure(
            Errc::runtime_already_exists);
    }
    active_runtime_pid.store(process, std::memory_order_release);

    auto started = runtime->start();
    if (!started) {
        expected = runtime.get();
        active_runtime.compare_exchange_strong(
            expected, nullptr, std::memory_order_acq_rel);
        return Result<std::unique_ptr<Runtime>>::failure(started.error());
    }
    return Result<std::unique_ptr<Runtime>>::success(std::move(runtime));
}

Result<void> Runtime::start() noexcept {
    impl_->running.store(true, std::memory_order_release);
    std::error_code start_error;

    try {
        for (std::size_t i = 0; i < impl_->workers.size(); ++i) {
            impl_->workers[i]->thread =
                std::thread([this, i] { impl_->worker_loop(i); });

            cpu_set_t mask;
            CPU_ZERO(&mask);
            CPU_SET(impl_->cpus[i], &mask);
            const int error = ::pthread_setaffinity_np(
                impl_->workers[i]->thread.native_handle(), sizeof(mask), &mask);
            if (error != 0) {
                start_error = pthread_error(error);
                break;
            }
        }
    } catch (const std::system_error& error) {
        start_error = error.code();
    } catch (...) {
        std::terminate();
    }

    {
        std::lock_guard lock(impl_->start_mutex);
        impl_->abort_start = static_cast<bool>(start_error);
        impl_->start_gate = true;
    }
    impl_->start_cv.notify_all();

    if (start_error) {
        for (auto& worker : impl_->workers) {
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
        impl_->running.store(false, std::memory_order_release);
        impl_->joined.store(true, std::memory_order_release);
        return Result<void>::failure(start_error);
    }
    return Result<void>::success();
}

Runtime::~Runtime() {
    if (impl_) {
        auto joined = join();
        if (!joined && joined.error() == make_error_code(Errc::join_from_worker)) {
            std::terminate();
        }
    }

    Runtime* expected = this;
    active_runtime.compare_exchange_strong(
        expected, nullptr, std::memory_order_acq_rel);
}

Result<void> Runtime::submit_root(
    std::shared_ptr<detail::CompletionStateBase> state,
    std::coroutine_handle<> coroutine) noexcept {
    std::size_t worker = 0;
    {
        std::lock_guard lock(impl_->roots_mutex);
        if (!impl_->accepting.load(std::memory_order_relaxed)) {
            return Result<void>::failure(Errc::runtime_stopping);
        }
        impl_->roots.emplace(state.get(), state);
        impl_->pending_roots.fetch_add(1, std::memory_order_release);
        worker = impl_->round_robin.fetch_add(1, std::memory_order_relaxed) %
                 impl_->workers.size();
    }

    if (!schedule_internal(coroutine, worker, 0, false)) {
        std::lock_guard lock(impl_->roots_mutex);
        impl_->roots.erase(state.get());
        impl_->pending_roots.fetch_sub(1, std::memory_order_release);
        return Result<void>::failure(Errc::runtime_stopping);
    }
    return Result<void>::success();
}

void Runtime::request_stop() noexcept {
    {
        std::lock_guard lock(impl_->roots_mutex);
        impl_->accepting.store(false, std::memory_order_release);
        impl_->stop_requested.store(true, std::memory_order_release);
    }
    impl_->wake_all();
}

Result<void> Runtime::join() noexcept {
    if (current() == this) {
        return Result<void>::failure(Errc::join_from_worker);
    }

    std::lock_guard join_lock(impl_->join_mutex);
    if (impl_->joined.load(std::memory_order_acquire)) {
        return Result<void>::success();
    }

    request_stop();
    for (auto& worker : impl_->workers) {
        if (worker->thread.joinable()) {
            worker->thread.join();
        }
    }
    impl_->running.store(false, std::memory_order_release);
    impl_->joined.store(true, std::memory_order_release);
    impl_->reap_completed();
    return Result<void>::success();
}

std::size_t Runtime::worker_count() const noexcept {
    return impl_->workers.size();
}

std::size_t Runtime::owner_for(std::uint64_t key) const noexcept {
    // A fixed integer hash avoids depending on an implementation-defined
    // std::hash salt while still spreading sequential inode IDs.
    key ^= key >> 33U;
    key *= 0xff51afd7ed558ccdULL;
    key ^= key >> 33U;
    key *= 0xc4ceb9fe1a85ec53ULL;
    key ^= key >> 33U;
    return static_cast<std::size_t>(key % impl_->workers.size());
}

Runtime* Runtime::current() noexcept {
    return tls_runtime;
}

std::size_t Runtime::current_worker() noexcept {
    return tls_worker;
}

bool Runtime::schedule(std::coroutine_handle<> coroutine,
                       std::optional<std::size_t> worker) noexcept {
    if (!coroutine || impl_->workers.empty()) {
        return false;
    }
    detail::ResumeTarget pinned;
    {
        std::lock_guard lock(impl_->pins_mutex);
        auto found = impl_->pinned_coroutines.find(coroutine.address());
        if (found != impl_->pinned_coroutines.end()) {
            pinned = found->second;
        }
    }

    if (pinned.runtime != nullptr && worker && *worker != pinned.worker) {
        return false;
    }
    const std::size_t target = pinned.runtime != nullptr
                                   ? pinned.worker
                                   : worker.value_or(
                                         impl_->round_robin.fetch_add(
                                             1, std::memory_order_relaxed) %
                                         impl_->workers.size());
    if (target >= impl_->workers.size()) {
        return false;
    }
    return schedule_internal(coroutine, target, pinned.pin_depth,
                             worker.has_value() || pinned.owner ||
                                 pinned.pin_depth != 0);
}

bool Runtime::schedule_internal(std::coroutine_handle<> coroutine,
                                std::size_t worker,
                                unsigned pin_depth,
                                bool owner) noexcept {
    if (!coroutine || worker >= impl_->workers.size()) {
        return false;
    }

    std::shared_lock admission_lock(impl_->schedule_mutex);
    if (impl_->scheduling_closed ||
        !impl_->running.load(std::memory_order_acquire) ||
        impl_->joined.load(std::memory_order_acquire)) {
        return false;
    }

    WorkItem item{coroutine, pin_depth, owner || pin_depth != 0};
    impl_->scheduled_items.fetch_add(1, std::memory_order_acq_rel);

    auto& target = *impl_->workers[worker];
    if (tls_runtime == this && tls_worker == worker) {
        std::lock_guard lock(target.queue_mutex);
        if (item.owner) {
            target.owner_fifo.push_back(std::move(item));
        } else {
            target.stealable.push_back(std::move(item));
        }
    } else {
        std::lock_guard lock(target.inbox_mutex);
        target.inbox.push_back(std::move(item));
    }
    impl_->wake_one();
    return true;
}

void Runtime::register_pin(std::coroutine_handle<> coroutine,
                           detail::ResumeTarget target) noexcept {
    if (!coroutine || target.runtime != this ||
        target.worker >= impl_->workers.size() || target.pin_depth == 0) {
        std::terminate();
    }
    std::lock_guard lock(impl_->pins_mutex);
    impl_->pinned_coroutines[coroutine.address()] = target;
}

void Runtime::transfer_pin(std::coroutine_handle<> coroutine,
                           std::coroutine_handle<> continuation) noexcept {
    if (!coroutine || !continuation || tls_runtime != this ||
        tls_worker >= impl_->workers.size()) {
        std::terminate();
    }
    std::lock_guard lock(impl_->pins_mutex);
    impl_->pinned_coroutines.erase(coroutine.address());
    if (tls_pin_depth == 0) {
        impl_->pinned_coroutines.erase(continuation.address());
    } else {
        impl_->pinned_coroutines[continuation.address()] =
            {this, tls_worker, tls_pin_depth, tls_owner};
    }
}

void Runtime::unregister_pin(std::coroutine_handle<> coroutine) noexcept {
    if (!coroutine) {
        std::terminate();
    }
    std::lock_guard lock(impl_->pins_mutex);
    if (tls_pin_depth == 0) {
        impl_->pinned_coroutines.erase(coroutine.address());
        return;
    }
    impl_->pinned_coroutines[coroutine.address()] =
        {this, tls_worker, tls_pin_depth, tls_owner};
}

void Runtime::on_root_completed(detail::CompletionStateBase* state) noexcept {
    if (tls_runtime != this || tls_worker >= impl_->workers.size()) {
        std::terminate();
    }
    // Destruction is deferred until the outermost worker resume() returns;
    // final_suspend is still executing at this point.
    impl_->workers[tls_worker]->deferred_completed.push_back(state);
    impl_->pending_roots.fetch_sub(1, std::memory_order_acq_rel);
    impl_->wake_all();
}

detail::UnobservedErrorHandler Runtime::unobserved_error_handler() const {
    return impl_->options.on_unobserved_error;
}

bool Runtime::ScheduleOnAwaiter::await_suspend(
    std::coroutine_handle<> coroutine) noexcept {
    if (worker_ >= runtime_->worker_count()) {
        error_ = make_error_code(Errc::invalid_worker);
        return false;
    }

    const auto current_target = detail::current_resume_target();
    if (current_target.runtime != nullptr &&
        current_target.runtime != runtime_) {
        error_ = make_error_code(Errc::wrong_runtime);
        return false;
    }
    if (current_target.runtime == runtime_ && current_target.pin_depth != 0 &&
        current_target.worker != worker_) {
        error_ = make_error_code(Errc::pinned_to_worker);
        return false;
    }
    if (current_target.runtime == runtime_ &&
        current_target.worker == worker_) {
        tls_owner = true;
        return false;
    }

    if (!runtime_->schedule_internal(
            coroutine, worker_, current_target.pin_depth, true)) {
        error_ = make_error_code(Errc::runtime_stopping);
        return false;
    }
    return true;
}

Result<void> Runtime::ScheduleOnAwaiter::await_resume() const noexcept {
    return error_ ? Result<void>::failure(error_) : Result<void>::success();
}

bool Runtime::YieldAwaiter::await_suspend(
    std::coroutine_handle<> coroutine) noexcept {
    const auto target = detail::current_resume_target();
    if (target.runtime == nullptr || target.worker == detail::no_worker) {
        error_ = make_error_code(Errc::not_in_runtime);
        return false;
    }
    if (!target.runtime->schedule_internal(
            coroutine, target.worker, target.pin_depth,
            target.owner || target.pin_depth != 0)) {
        error_ = make_error_code(Errc::runtime_stopping);
        return false;
    }
    return true;
}

Result<void> Runtime::YieldAwaiter::await_resume() const noexcept {
    return error_ ? Result<void>::failure(error_) : Result<void>::success();
}

namespace detail {

ResumeTarget current_resume_target() noexcept {
    return {tls_runtime, tls_worker, tls_pin_depth, tls_owner};
}

void schedule_resume(ResumeTarget target,
                     std::coroutine_handle<> coroutine,
                     bool owner) noexcept {
    if (!coroutine) {
        return;
    }
    if (target.runtime == nullptr) {
        coroutine.resume();
        return;
    }
    if (target.worker == no_worker) {
        target.worker = target.runtime->owner_for(
            reinterpret_cast<std::uintptr_t>(coroutine.address()));
    }
    if (!target.runtime->schedule_internal(
            coroutine, target.worker, target.pin_depth,
            owner || target.owner || target.pin_depth != 0)) {
        std::terminate();
    }
}

void prepare_symmetric_resume(ResumeTarget target) noexcept {
    tls_runtime = target.runtime;
    tls_worker = target.worker;
    tls_pin_depth = target.pin_depth;
    tls_owner = target.owner;
}

void pin_current(std::coroutine_handle<> coroutine) noexcept {
    if (tls_runtime == nullptr || tls_worker == no_worker) {
        std::terminate();
    }
    ++tls_pin_depth;
    tls_runtime->register_pin(
        coroutine, {tls_runtime, tls_worker, tls_pin_depth, tls_owner});
}

void register_pin(std::coroutine_handle<> coroutine,
                  ResumeTarget target) noexcept {
    if (target.runtime == nullptr) {
        std::terminate();
    }
    target.runtime->register_pin(coroutine, target);
}

void transfer_pin_to_continuation(
    std::coroutine_handle<> coroutine,
    std::coroutine_handle<> continuation) noexcept {
    if (tls_runtime == nullptr || tls_worker == no_worker) {
        // Tasks can be driven manually outside Runtime when no range pin is
        // involved; there is no process scheduler registry to update.
        return;
    }
    if (!coroutine || !continuation) {
        std::terminate();
    }
    tls_runtime->transfer_pin(coroutine, continuation);
}

void unpin_current(std::coroutine_handle<> coroutine) noexcept {
    if (tls_runtime == nullptr || tls_worker == no_worker ||
        tls_pin_depth == 0) {
        std::terminate();
    }
    --tls_pin_depth;
    tls_runtime->unregister_pin(coroutine);
}

void root_completed(Runtime* runtime, CompletionStateBase* state) noexcept {
    if (runtime == nullptr || state == nullptr) {
        std::terminate();
    }
    runtime->on_root_completed(state);
}

} // namespace detail
} // namespace orchfs::async
