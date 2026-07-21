#include "orchfs/async/range_arbiter.hpp"
#include "orchfs/async/runtime.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string_view>
#include <thread>
#include <type_traits>

#include <pthread.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

using namespace std::chrono_literals;
using orchfs::async::Errc;
using orchfs::async::JoinHandle;
using orchfs::async::RangeArbiter;
using orchfs::async::RangeMode;
using orchfs::async::Result;
using orchfs::async::Runtime;
using orchfs::async::RuntimeOptions;
using orchfs::async::Task;

namespace {

[[noreturn]] void fail(std::string_view message) {
    std::cerr << "async runtime test failed: " << message << '\n';
    std::abort();
}

void check(bool condition, std::string_view message) {
    if (!condition) {
        fail(message);
    }
}

[[nodiscard]] std::chrono::nanoseconds thread_cpu_time(pthread_t thread) {
    clockid_t clock{};
    check(::pthread_getcpuclockid(thread, &clock) == 0,
          "worker CPU clock lookup");

    timespec value{};
    check(::clock_gettime(clock, &value) == 0, "worker CPU clock sample");
    return std::chrono::seconds(value.tv_sec) +
           std::chrono::nanoseconds(value.tv_nsec);
}

template <typename Predicate>
void wait_until(Predicate&& predicate, std::string_view message) {
    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            fail(message);
        }
        std::this_thread::sleep_for(1ms);
    }
}

Task<int> answer_task() {
    co_return 42;
}

Task<Result<std::size_t>> owner_task(Runtime& runtime, std::size_t owner) {
    auto scheduled = co_await runtime.schedule_on(owner);
    if (!scheduled) {
        co_return Result<std::size_t>::failure(scheduled.error());
    }
    if (Runtime::current() != &runtime || Runtime::current_worker() != owner) {
        co_return Result<std::size_t>::failure(Errc::wrong_runtime);
    }
    auto yielded = co_await Runtime::yield();
    if (!yielded) {
        co_return Result<std::size_t>::failure(yielded.error());
    }
    co_return Result<std::size_t>::success(Runtime::current_worker());
}

Task<Result<void>> compatible_reader(RangeArbiter& arbiter,
                                     std::atomic<unsigned>& entered,
                                     std::atomic<bool>& release) {
    auto acquired = co_await arbiter.acquire(17, 4096, RangeMode::read);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    entered.fetch_add(1, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
        auto yielded = co_await Runtime::yield();
        if (!yielded) {
            co_return Result<void>::failure(yielded.error());
        }
    }
    auto released = co_await permit.release();
    co_return released;
}

Task<Result<void>> leading_reader(RangeArbiter& arbiter,
                                  std::atomic<bool>& entered,
                                  std::atomic<bool>& release) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::read);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    entered.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
        (void)co_await Runtime::yield();
    }
    co_return co_await permit.release();
}

Task<Result<void>> phased_writer(RangeArbiter& arbiter,
                                 std::atomic<unsigned>& order,
                                 std::atomic<bool>& entered,
                                 std::atomic<bool>& release) {
    auto acquired = co_await arbiter.acquire(1024, 1024, RangeMode::write);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    unsigned expected = 0;
    check(order.compare_exchange_strong(expected, 1),
          "writer did not enter first phase");
    entered.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) {
        (void)co_await Runtime::yield();
    }
    co_return co_await permit.release();
}

Task<Result<void>> late_reader(RangeArbiter& arbiter,
                               std::atomic<unsigned>& order,
                               std::atomic<bool>& entered) {
    auto acquired = co_await arbiter.acquire(2048, 1, RangeMode::read);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    unsigned expected = 1;
    check(order.compare_exchange_strong(expected, 2),
          "late reader bypassed queued writer");
    entered.store(true, std::memory_order_release);
    co_return co_await permit.release();
}

Task<Result<void>> pinning_task(Runtime& runtime, RangeArbiter& arbiter) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::write);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    check(permit.first_block() == 0 && permit.block_count() == 1,
          "32 KiB normalization failed");

    const std::size_t pinned_worker = Runtime::current_worker();
    for (unsigned i = 0; i < 32; ++i) {
        auto yielded = co_await Runtime::yield();
        if (!yielded || Runtime::current_worker() != pinned_worker) {
            co_return Result<void>::failure(Errc::pinned_to_worker);
        }
    }

    const std::size_t other = (pinned_worker + 1) % runtime.worker_count();
    auto migrate = co_await runtime.schedule_on(other);
    if (other != pinned_worker &&
        migrate.error() != orchfs::async::make_error_code(Errc::pinned_to_worker)) {
        co_return Result<void>::failure(Errc::pinned_to_worker);
    }
    co_return co_await permit.release();
}

Task<Result<int>> unobserved_failure_task() {
    co_return Result<int>::failure(std::make_error_code(std::errc::io_error));
}

Task<void> detached_task(std::atomic<bool>& completed) {
    for (unsigned i = 0; i < 8; ++i) {
        (void)co_await Runtime::yield();
    }
    completed.store(true, std::memory_order_release);
}

struct ExternalResume {
    std::mutex mutex;
    std::coroutine_handle<> coroutine{};
    pthread_t suspended_thread{};

    struct Awaiter {
        ExternalResume& state;

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle) noexcept {
            std::lock_guard lock(state.mutex);
            state.suspended_thread = ::pthread_self();
            state.coroutine = handle;
        }

        void await_resume() const noexcept {}
    };

    [[nodiscard]] Awaiter wait() noexcept {
        return {*this};
    }

    [[nodiscard]] std::coroutine_handle<> take() noexcept {
        std::lock_guard lock(mutex);
        return std::exchange(coroutine, {});
    }

    [[nodiscard]] bool ready() noexcept {
        std::lock_guard lock(mutex);
        return static_cast<bool>(coroutine);
    }

    [[nodiscard]] pthread_t worker_thread() noexcept {
        std::lock_guard lock(mutex);
        return suspended_thread;
    }
};

Task<std::size_t> externally_resumed_task(ExternalResume& resume) {
    co_await resume.wait();
    co_return Runtime::current_worker();
}

Task<Result<orchfs::async::RangePermit>> acquire_in_nested_task(
    RangeArbiter& arbiter) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::write);
    if (!acquired) {
        co_return Result<orchfs::async::RangePermit>::failure(
            acquired.error());
    }
    co_return Result<orchfs::async::RangePermit>::success(
        std::move(acquired).value());
}

Task<Result<void>> returned_permit_survives_suspend(
    RangeArbiter& arbiter, ExternalResume& while_pinned,
    ExternalResume& after_release,
    std::atomic<std::size_t>& pinned_worker) {
    auto acquired = co_await acquire_in_nested_task(arbiter);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    const std::size_t owner = Runtime::current_worker();
    pinned_worker.store(owner, std::memory_order_release);
    co_await while_pinned.wait();
    if (Runtime::current_worker() != owner) {
        co_return Result<void>::failure(Errc::pinned_to_worker);
    }
    auto released = co_await permit.release();
    if (!released) {
        co_return released;
    }
    co_await after_release.wait();
    if (Runtime::current_worker() == owner) {
        co_return Result<void>::failure(Errc::pinned_to_worker);
    }
    co_return Result<void>::success();
}

Task<Result<void>> externally_resumed_while_pinned(
    RangeArbiter& arbiter, ExternalResume& resume,
    std::atomic<std::size_t>& pinned_worker) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::write);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    const std::size_t owner = Runtime::current_worker();
    pinned_worker.store(owner, std::memory_order_release);
    co_await resume.wait();
    if (Runtime::current_worker() != owner) {
        co_return Result<void>::failure(Errc::pinned_to_worker);
    }
    co_return co_await permit.release();
}

Task<Result<void>> nested_external_suspend(
    ExternalResume& resume, std::size_t expected_worker) {
    co_await resume.wait();
    if (Runtime::current_worker() != expected_worker) {
        co_return Result<void>::failure(Errc::pinned_to_worker);
    }
    co_return Result<void>::success();
}

Task<Result<void>> nested_while_parent_pinned(
    RangeArbiter& arbiter, ExternalResume& resume,
    std::atomic<std::size_t>& pinned_worker) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::write);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    const std::size_t owner = Runtime::current_worker();
    pinned_worker.store(owner, std::memory_order_release);
    auto nested = co_await nested_external_suspend(resume, owner);
    if (!nested) {
        auto released = co_await permit.release();
        (void)released;
        co_return nested;
    }
    co_return co_await permit.release();
}

Task<Result<void>> discard_release_awaiter(RangeArbiter& arbiter) {
    auto acquired = co_await arbiter.acquire(0, 1, RangeMode::write);
    if (!acquired) {
        co_return Result<void>::failure(acquired.error());
    }
    auto permit = std::move(acquired).value();
    [[maybe_unused]] auto abandoned = permit.release();
    co_return Result<void>::success();
}

struct RuntimePollProbe {
    Runtime* runtime{};
    std::size_t worker{};
    std::atomic<unsigned> calls{0};
    std::atomic<bool> busy{true};
    std::atomic<bool> wrong_worker{false};

    static Runtime::PollState poll(void* context) noexcept {
        auto& probe = *static_cast<RuntimePollProbe*>(context);
        if (Runtime::current() != probe.runtime ||
            Runtime::current_worker() != probe.worker) {
            probe.wrong_worker.store(true, std::memory_order_release);
        }
        const unsigned call =
            probe.calls.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (call == 1) {
            return Runtime::PollState::progress;
        }
        return probe.busy.load(std::memory_order_acquire)
                   ? Runtime::PollState::busy
                   : Runtime::PollState::idle;
    }
};

void require_ok(Result<Result<void>> outer, std::string_view message) {
    check(static_cast<bool>(outer), message);
    auto inner = std::move(outer).value();
    check(static_cast<bool>(inner), message);
}

} // namespace

int main() {
    static_assert(!std::is_copy_constructible_v<Task<int>>);
    static_assert(!std::is_copy_constructible_v<JoinHandle<int>>);

    std::atomic<unsigned> unobserved_errors{0};
    RuntimeOptions options;
    options.worker_count = 4;
    options.spin_before_park = 64;
    options.on_unobserved_error = [&](std::error_code error) {
        if (error == std::make_error_code(std::errc::io_error)) {
            unobserved_errors.fetch_add(1, std::memory_order_release);
        }
    };

    auto created = Runtime::create(std::move(options));
    check(static_cast<bool>(created), "runtime create");
    auto runtime = std::move(created).value();
    check(runtime->worker_count() == 4, "worker count");
    for (std::size_t worker = 0; worker < runtime->worker_count(); ++worker) {
        auto cpu = runtime->worker_cpu(worker);
        check(static_cast<bool>(cpu), "worker CPU lookup");
    }

    RuntimePollProbe poll_probe{
        .runtime = runtime.get(),
        .worker = runtime->owner_for(0xfeedU),
    };
    auto registered = runtime->register_poller(
        poll_probe.worker, RuntimePollProbe::poll, &poll_probe);
    check(static_cast<bool>(registered), "worker poller registration");
    auto poll_registration = std::move(registered).value();
    wait_until([&] { return poll_probe.calls.load() >= 32; },
               "worker poller did not run");
    check(!poll_probe.wrong_worker.load(), "poller ran on wrong worker");
    poll_probe.busy.store(false, std::memory_order_release);
    poll_registration.reset();
    const unsigned calls_after_reset = poll_probe.calls.load();
    std::this_thread::sleep_for(20ms);
    check(poll_probe.calls.load() == calls_after_reset,
          "retired poller was called again");

    auto duplicate = Runtime::create();
    check(!duplicate &&
              duplicate.error() ==
                  orchfs::async::make_error_code(Errc::runtime_already_exists),
          "process-wide runtime uniqueness");

    auto answer = runtime->submit(answer_task());
    check(static_cast<bool>(answer), "answer submit");
    auto answer_result = std::move(answer.value()).join();
    check(answer_result && answer_result.value() == 42, "answer join");

    const std::size_t owner = runtime->owner_for(0x12345678U);
    auto owned = runtime->submit(owner_task(*runtime, owner));
    check(static_cast<bool>(owned), "owner task submit");
    auto owned_outer = std::move(owned.value()).join();
    check(static_cast<bool>(owned_outer), "owner task join");
    auto owned_inner = std::move(owned_outer).value();
    check(owned_inner && owned_inner.value() == owner, "schedule_on owner");

    RangeArbiter compatible_arbiter;
    std::atomic<unsigned> readers_entered{0};
    std::atomic<bool> release_readers{false};
    auto reader_one = runtime->submit(
        compatible_reader(compatible_arbiter, readers_entered,
                          release_readers));
    auto reader_two = runtime->submit(
        compatible_reader(compatible_arbiter, readers_entered,
                          release_readers));
    check(reader_one && reader_two, "compatible readers submit");
    wait_until([&] { return readers_entered.load() == 2; },
               "overlapping readers were not compatible");
    release_readers.store(true, std::memory_order_release);
    require_ok(std::move(reader_one.value()).join(), "reader one");
    require_ok(std::move(reader_two.value()).join(), "reader two");

    RangeArbiter phase_arbiter;
    std::atomic<bool> first_entered{false};
    std::atomic<bool> release_first{false};
    std::atomic<bool> writer_entered{false};
    std::atomic<bool> release_writer{false};
    std::atomic<bool> late_entered{false};
    std::atomic<unsigned> order{0};

    auto first = runtime->submit(
        leading_reader(phase_arbiter, first_entered, release_first));
    check(static_cast<bool>(first), "leading reader submit");
    wait_until([&] { return first_entered.load(); }, "leading reader enter");

    auto writer = runtime->submit(phased_writer(
        phase_arbiter, order, writer_entered, release_writer));
    check(static_cast<bool>(writer), "writer submit");
    wait_until([&] { return phase_arbiter.waiting_count() == 1; },
               "writer queue");

    auto late = runtime->submit(late_reader(phase_arbiter, order, late_entered));
    check(static_cast<bool>(late), "late reader submit");
    wait_until([&] { return phase_arbiter.waiting_count() == 2; },
               "late reader queue");

    release_first.store(true, std::memory_order_release);
    wait_until([&] { return writer_entered.load(); }, "writer handoff");
    check(!late_entered.load(), "late reader bypassed writer phase");
    release_writer.store(true, std::memory_order_release);
    wait_until([&] { return late_entered.load(); }, "late reader handoff");
    require_ok(std::move(first.value()).join(), "leading reader");
    require_ok(std::move(writer.value()).join(), "writer");
    require_ok(std::move(late.value()).join(), "late reader");

    RangeArbiter pin_arbiter;
    auto pinned = runtime->submit(pinning_task(*runtime, pin_arbiter));
    check(static_cast<bool>(pinned), "pinning task submit");
    require_ok(std::move(pinned.value()).join(), "pinning task");

    std::atomic<bool> detached_completed{false};
    {
        auto detached = runtime->submit(detached_task(detached_completed));
        check(static_cast<bool>(detached), "detached task submit");
    }
    wait_until([&] { return detached_completed.load(); },
               "dropped JoinHandle cancelled task");

    {
        auto unobserved = runtime->submit(unobserved_failure_task());
        check(static_cast<bool>(unobserved), "unobserved task submit");
    }
    wait_until([&] { return unobserved_errors.load() == 1; },
               "unobserved error callback");

    RangeArbiter external_pin_arbiter;
    ExternalResume pinned_external;
    std::atomic<std::size_t> external_pin_worker{
        orchfs::async::detail::no_worker};
    auto pinned_io = runtime->submit(externally_resumed_while_pinned(
        external_pin_arbiter, pinned_external, external_pin_worker));
    check(static_cast<bool>(pinned_io), "pinned external task submit");
    wait_until([&] { return pinned_external.ready(); },
               "pinned external suspension");
    const auto pinned_target =
        external_pin_worker.load(std::memory_order_acquire);
    check(runtime->schedule(pinned_external.take(), pinned_target),
          "pinned external completion schedule");
    require_ok(std::move(pinned_io.value()).join(),
               "pinned external completion");

    RangeArbiter nested_pin_arbiter;
    ExternalResume nested_external;
    std::atomic<std::size_t> nested_pin_worker{
        orchfs::async::detail::no_worker};
    auto nested_pinned = runtime->submit(nested_while_parent_pinned(
        nested_pin_arbiter, nested_external, nested_pin_worker));
    check(static_cast<bool>(nested_pinned), "nested pinned task submit");
    wait_until([&] { return nested_external.ready(); },
               "nested pinned suspension");
    const auto nested_target =
        nested_pin_worker.load(std::memory_order_acquire);
    const auto wrong_nested_target =
        (nested_target + 1) % runtime->worker_count();
    auto nested_coroutine = nested_external.take();
    check(!runtime->schedule(nested_coroutine, wrong_nested_target),
          "nested coroutine escaped parent pin");
    check(runtime->schedule(nested_coroutine, nested_target),
          "nested pinned completion schedule");
    require_ok(std::move(nested_pinned.value()).join(),
               "nested pinned completion");

    RangeArbiter returned_permit_arbiter;
    ExternalResume returned_permit_external;
    ExternalResume returned_permit_after_release;
    std::atomic<std::size_t> returned_permit_worker{
        orchfs::async::detail::no_worker};
    auto returned_permit = runtime->submit(returned_permit_survives_suspend(
        returned_permit_arbiter, returned_permit_external,
        returned_permit_after_release,
        returned_permit_worker));
    check(static_cast<bool>(returned_permit), "returned permit task submit");
    wait_until([&] { return returned_permit_external.ready(); },
               "returned permit external suspension");
    const auto returned_target =
        returned_permit_worker.load(std::memory_order_acquire);
    check(runtime->schedule(returned_permit_external.take(), returned_target),
          "returned permit completion schedule");
    wait_until([&] { return returned_permit_after_release.ready(); },
               "returned permit release suspension");
    const auto unpinned_target =
        (returned_target + 1) % runtime->worker_count();
    check(runtime->schedule(returned_permit_after_release.take(),
                            unpinned_target),
          "returned permit left a stale parent pin");
    require_ok(std::move(returned_permit.value()).join(),
               "returned nested permit completion");

    // fork() preserves the parent's singleton pointer but none of its worker
    // threads. The child must abandon that inherited Runtime and create a new
    // process-local one without disturbing the still-live parent instance.
    const pid_t child = ::fork();
    check(child >= 0, "runtime fork");
    if (child == 0) {
        RuntimeOptions child_options;
        child_options.worker_count = 1;
        auto child_created = Runtime::create(std::move(child_options));
        if (!child_created) {
            _exit(20);
        }
        auto child_runtime = std::move(child_created).value();
        auto child_answer = child_runtime->submit(answer_task());
        if (!child_answer) {
            _exit(21);
        }
        auto child_joined = std::move(child_answer).value().join();
        if (!child_joined || child_joined.value() != 42) {
            _exit(22);
        }
        child_runtime->request_stop();
        if (!child_runtime->join()) {
            _exit(23);
        }
        child_runtime.reset();
        _exit(0);
    }
    int child_status = 0;
    check(::waitpid(child, &child_status, 0) == child,
          "runtime child wait");
    check(WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0,
          "runtime child recreate after fork");

    // release() transfers ownership into its awaiter. Discarding that awaiter
    // must fail fast instead of silently retaining the active range and pin.
    const pid_t release_child = ::fork();
    check(release_child >= 0, "release awaiter fork");
    if (release_child == 0) {
        std::set_terminate([] { _exit(24); });
        RuntimeOptions child_options;
        child_options.worker_count = 1;
        auto child_created = Runtime::create(std::move(child_options));
        if (!child_created) {
            _exit(25);
        }
        auto child_runtime = std::move(child_created).value();
        RangeArbiter child_arbiter;
        auto submitted =
            child_runtime->submit(discard_release_awaiter(child_arbiter));
        if (!submitted) {
            _exit(26);
        }
        (void)std::move(submitted).value().join();
        _exit(27);
    }
    int release_child_status = 0;
    check(::waitpid(release_child, &release_child_status, 0) == release_child,
          "release awaiter child wait");
    check(WIFEXITED(release_child_status) &&
              WEXITSTATUS(release_child_status) == 24,
          "discarded release awaiter did not terminate");

    ExternalResume external;
    auto externally_resumed = runtime->submit(externally_resumed_task(external));
    check(static_cast<bool>(externally_resumed), "external task submit");
    wait_until([&] { return external.ready(); }, "external suspension");

    runtime->request_stop();
    auto rejected = runtime->submit(answer_task());
    check(!rejected &&
              rejected.error() ==
                  orchfs::async::make_error_code(Errc::runtime_stopping),
          "submit after stop");

    const std::size_t completion_worker = runtime->owner_for(0xabcU);
    check(runtime->schedule(external.take(), completion_worker),
          "completion schedule during drain");
    auto external_result = std::move(externally_resumed.value()).join();
    check(external_result && external_result.value() == completion_worker,
          "directed completion worker");

    auto joined = runtime->join();
    check(static_cast<bool>(joined), "runtime join");
    check(!runtime->schedule(std::noop_coroutine()),
          "schedule accepted after join");

    // A stopped Runtime with an outstanding external completion must park its
    // idle workers.  Sampling the worker's own CPU clock makes this independent
    // of scheduler latency on the test thread: the historical stop predicate
    // returned immediately forever and consumed essentially the whole sample
    // interval in this state.
    runtime.reset();
    RuntimeOptions parked_options;
    parked_options.worker_count = 1;
    parked_options.spin_before_park = 0;
    auto parked_created = Runtime::create(std::move(parked_options));
    check(static_cast<bool>(parked_created), "parked runtime create");
    auto parked_runtime = std::move(parked_created).value();
    ExternalResume parked_external;
    auto parked_root =
        parked_runtime->submit(externally_resumed_task(parked_external));
    check(static_cast<bool>(parked_root), "parked root submit");
    wait_until([&] { return parked_external.ready(); },
               "parked root external suspension");
    const pthread_t parked_worker = parked_external.worker_thread();
    parked_runtime->request_stop();
    const auto parked_cpu_before = thread_cpu_time(parked_worker);
    std::this_thread::sleep_for(500ms);
    const auto parked_cpu_after = thread_cpu_time(parked_worker);
    check(parked_cpu_after - parked_cpu_before < 100ms,
          "stopped worker busy-spun while awaiting external completion");
    check(parked_runtime->schedule(parked_external.take(), 0),
          "parked completion schedule during drain");
    auto parked_result = std::move(parked_root.value()).join();
    check(parked_result && parked_result.value() == 0,
          "parked external completion worker");
    check(static_cast<bool>(parked_runtime->join()), "parked runtime join");
    parked_runtime.reset();

    // A stopped Runtime closes completion admission when the final worker
    // observes a fully drained scheduler, not later when join() publishes its
    // result.  Repeatedly yield admission to the worker so this also exercises
    // the schedule-vs-final-exit race: the historical implementation kept
    // returning true after the worker had already exited and stranded every
    // subsequently accepted coroutine.
    RuntimeOptions drain_options;
    drain_options.worker_count = 1;
    drain_options.spin_before_park = 0;
    auto drain_created = Runtime::create(std::move(drain_options));
    check(static_cast<bool>(drain_created), "drain runtime create");
    auto drain_runtime = std::move(drain_created).value();
    drain_runtime->request_stop();
    bool admission_closed = false;
    const auto admission_deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < admission_deadline) {
        if (!drain_runtime->schedule(std::noop_coroutine())) {
            admission_closed = true;
            break;
        }
        std::this_thread::sleep_for(100us);
    }
    check(admission_closed, "schedule admitted work after final worker exit");
    check(static_cast<bool>(drain_runtime->join()), "drain runtime join");

    std::cout << "async runtime tests passed\n";
    return 0;
}
