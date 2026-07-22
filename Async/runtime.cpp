#include "orchfs/async/runtime.hpp"

#include "orchfs/async/detail/concurrency.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <deque>
#include <immintrin.h>
#include <new>
#include <pthread.h>
#include <sched.h>
#include <thread>
#include <unordered_map>
#include <linux/futex.h>
#include <sys/syscall.h>
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

struct WorkNode {
    WorkItem item;
    WorkNode* next{};
    bool pooled{};
    bool remote_pooled{};
    std::atomic<bool> remote_claimed{false};
};

class CoroutineFramePool final {
public:
    CoroutineFramePool() {
        for (std::size_t index = 0; index < buckets_.size(); ++index) {
            initialize_bucket(index, kPayloadSizes[index], kSlotCounts[index]);
        }
    }

    [[nodiscard]] void* allocate(std::size_t size) {
        {
            detail::AtomicFlagGuard guard(lock_);
            for (std::size_t index = 0; index < buckets_.size(); ++index) {
                auto& bucket = buckets_[index];
                if (size > bucket.payload_size || bucket.free_head == nullptr) {
                    continue;
                }
                void* frame = bucket.free_head;
                bucket.free_head = *static_cast<void**>(frame);
                return frame;
            }
        }
        return allocate_heap(size);
    }

    static void release(void* frame) noexcept {
        if (frame == nullptr) {
            return;
        }
        auto* header = static_cast<FrameHeader*>(frame) - 1;
        if (header->pool == nullptr) {
            ::operator delete(header);
            return;
        }
        header->pool->release_pooled(frame, header->bucket);
    }

    static void* allocate_heap(std::size_t size) {
        auto* header = static_cast<FrameHeader*>(
            ::operator new(sizeof(FrameHeader) + size));
        *header = {};
        return header + 1;
    }

private:
    struct alignas(std::max_align_t) FrameHeader {
        CoroutineFramePool* pool{};
        std::size_t bucket{};
    };

    struct Bucket {
        std::size_t payload_size{};
        std::size_t stride{};
        std::unique_ptr<std::byte[]> storage;
        void* free_head{};
    };

    static constexpr std::array<std::size_t, 8> kPayloadSizes{
        128, 256, 512, 1024, 2048, 4096, 8192, 16384};
    static constexpr std::array<std::size_t, 8> kSlotCounts{
        512, 512, 256, 256, 128, 128, 128, 64};

    static constexpr std::size_t align_up(std::size_t size) noexcept {
        constexpr std::size_t alignment = alignof(std::max_align_t);
        return (size + alignment - 1U) & ~(alignment - 1U);
    }

    void initialize_bucket(std::size_t index, std::size_t payload_size,
                           std::size_t slot_count) {
        auto& bucket = buckets_[index];
        bucket.payload_size = payload_size;
        bucket.stride = align_up(sizeof(FrameHeader) + payload_size);
        bucket.storage =
            std::make_unique<std::byte[]>(bucket.stride * slot_count);
        for (std::size_t slot = 0; slot < slot_count; ++slot) {
            std::byte* block = bucket.storage.get() + slot * bucket.stride;
            auto* header = reinterpret_cast<FrameHeader*>(block);
            *header = FrameHeader{.pool = this, .bucket = index};
            void* frame = header + 1;
            *static_cast<void**>(frame) = bucket.free_head;
            bucket.free_head = frame;
        }
    }

    void release_pooled(void* frame, std::size_t bucket_index) noexcept {
        if (bucket_index >= buckets_.size()) {
            std::terminate();
        }
        detail::AtomicFlagGuard guard(lock_);
        auto& bucket = buckets_[bucket_index];
        *static_cast<void**>(frame) = bucket.free_head;
        bucket.free_head = frame;
    }

    std::array<Bucket, kPayloadSizes.size()> buckets_;
    std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

thread_local CoroutineFramePool* tls_frame_pool = nullptr;

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

void futex_wake(std::atomic<std::uint32_t>& epoch, int count) noexcept {
    static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
    (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&epoch),
                    FUTEX_WAKE_PRIVATE, count, nullptr, nullptr, 0);
}

void futex_wait_for(std::atomic<std::uint32_t>& epoch,
                    std::uint32_t observed) noexcept {
    static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
    constexpr struct timespec timeout {
        .tv_sec = 0,
        .tv_nsec = 10'000'000,
    };
    (void)::syscall(SYS_futex, reinterpret_cast<std::uint32_t*>(&epoch),
                    FUTEX_WAIT_PRIVATE, observed, &timeout, nullptr, 0);
}

} // namespace

struct Runtime::PollRegistration::State {
    static constexpr std::uint8_t kActive = 1U << 0U;

    struct Quiescence {
        std::atomic<std::uint64_t> poll_epoch{0};
        std::atomic<bool> exited{false};
        std::atomic<std::uint32_t> wake_epoch{0};
    };

    Runtime* runtime{};
    std::size_t worker{detail::no_worker};
    PollFunction function{};
    void* context{};
    std::atomic<std::uint8_t> lifecycle{kActive};
    std::shared_ptr<Quiescence> quiescence;
};

Runtime::PollRegistration::PollRegistration(PollRegistration&& other) noexcept
    : state_(std::exchange(other.state_, {})) {}

Runtime::PollRegistration& Runtime::PollRegistration::operator=(
    PollRegistration&& other) noexcept {
    if (this != &other) {
        reset();
        state_ = std::exchange(other.state_, {});
    }
    return *this;
}

Runtime::PollRegistration::~PollRegistration() {
    reset();
}

void Runtime::PollRegistration::reset() noexcept {
    auto state = std::exchange(state_, {});
    if (!state) {
        return;
    }
    state->lifecycle.fetch_and(
        static_cast<std::uint8_t>(~State::kActive),
        std::memory_order_acq_rel);
    if (Runtime::current() == state->runtime &&
        Runtime::current_worker() == state->worker) {
        // A worker may retire its own driver after the callback has returned.
        // Waiting here would deadlock if reset was requested from that callback.
        return;
    }
    const auto quiescence = state->quiescence;
    if (!quiescence) {
        return;
    }
    const std::uint64_t observed =
        quiescence->poll_epoch.load(std::memory_order_acquire);
    if (quiescence->exited.load(std::memory_order_acquire)) {
        return;
    }
    quiescence->wake_epoch.fetch_add(1, std::memory_order_release);
    futex_wake(quiescence->wake_epoch, 1);
    while (quiescence->poll_epoch.load(std::memory_order_acquire) == observed &&
           !quiescence->exited.load(std::memory_order_acquire)) {
        std::this_thread::yield();
    }
}

struct Runtime::Impl {
    using PollerList = std::vector<std::shared_ptr<PollRegistration::State>>;
    using RootRegistry =
        std::unordered_map<detail::CompletionStateBase*,
                           std::shared_ptr<detail::CompletionStateBase>>;

    class PinRegistry final {
    public:
        PinRegistry()
            : slots_(std::make_unique<Slot[]>(kCapacity)) {}

        [[nodiscard]] detail::ResumeTarget find(Runtime* runtime,
                                                 void* coroutine) const noexcept {
            if (coroutine == nullptr) {
                return {};
            }
            const std::size_t start = hash(coroutine);
            for (std::size_t probe = 0; probe < kCapacity; ++probe) {
                const auto& slot = slots_[(start + probe) & (kCapacity - 1)];
                void* const key = slot.coroutine.load(std::memory_order_acquire);
                if (key == coroutine) {
                    const std::uint64_t encoded =
                        slot.target.load(std::memory_order_acquire);
                    if (encoded == 0) {
                        return {};
                    }
                    return detail::ResumeTarget{
                        .runtime = runtime,
                        .worker = static_cast<std::size_t>(
                            (encoded & kWorkerMask) - 1),
                        .pin_depth = static_cast<unsigned>(
                            (encoded >> kDepthShift) & kDepthMask),
                        .owner = (encoded & kOwnerMask) != 0,
                    };
                }
                if (key == nullptr) {
                    return {};
                }
            }
            return {};
        }

        void set(void* coroutine, detail::ResumeTarget target) noexcept {
            if (coroutine == nullptr || target.runtime == nullptr ||
                target.worker >= kWorkerMask || target.pin_depth == 0 ||
                target.pin_depth > kDepthMask) {
                std::terminate();
            }
            const std::uint64_t encoded =
                static_cast<std::uint64_t>(target.worker + 1) |
                (static_cast<std::uint64_t>(target.pin_depth) << kDepthShift) |
                (target.owner ? kOwnerMask : 0);
            const std::size_t start = hash(coroutine);
            for (std::size_t probe = 0; probe < kCapacity; ++probe) {
                auto& slot = slots_[(start + probe) & (kCapacity - 1)];
                void* key = slot.coroutine.load(std::memory_order_acquire);
                if (key == coroutine) {
                    slot.target.store(encoded, std::memory_order_release);
                    return;
                }
                if (key == nullptr && slot.coroutine.compare_exchange_strong(
                                          key, coroutine,
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
                    slot.target.store(encoded, std::memory_order_release);
                    return;
                }
            }
            std::terminate();
        }

        void erase(void* coroutine) noexcept {
            if (coroutine == nullptr) {
                std::terminate();
            }
            const std::size_t start = hash(coroutine);
            for (std::size_t probe = 0; probe < kCapacity; ++probe) {
                auto& slot = slots_[(start + probe) & (kCapacity - 1)];
                void* const key = slot.coroutine.load(std::memory_order_acquire);
                if (key == coroutine) {
                    slot.target.store(0, std::memory_order_release);
                    return;
                }
                if (key == nullptr) {
                    return;
                }
            }
        }

    private:
        struct Slot {
            std::atomic<void*> coroutine{nullptr};
            std::atomic<std::uint64_t> target{0};
        };

        static constexpr std::size_t kCapacity = 1U << 18U;
        static constexpr std::uint64_t kWorkerMask = 0xffffffffULL;
        static constexpr unsigned kDepthShift = 32U;
        static constexpr std::uint64_t kDepthMask = 0x7fffffffULL;
        static constexpr std::uint64_t kOwnerMask = 1ULL << 63U;

        static std::size_t hash(void* coroutine) noexcept {
            const auto value = static_cast<std::uint64_t>(
                reinterpret_cast<std::uintptr_t>(coroutine));
            return static_cast<std::size_t>(detail::fmix64<false>(value)) &
                   (kCapacity - 1);
        }

        std::unique_ptr<Slot[]> slots_;
    };

    struct Worker {
        static constexpr std::size_t kWorkNodePoolSize = 4096;
        static constexpr std::size_t kRemoteWorkNodePoolSize = 4096;
        static_assert((kRemoteWorkNodePoolSize &
                       (kRemoteWorkNodePoolSize - 1U)) == 0U);

        detail::MpscInbox<WorkNode> inbox;
        WorkNode* local_head{};
        WorkNode* local_tail{};

        std::unique_ptr<WorkNode[]> work_node_pool;
        WorkNode* free_work_nodes{};
        std::unique_ptr<WorkNode[]> remote_work_node_pool;
        std::atomic<std::size_t> remote_work_node_cursor{0};
        CoroutineFramePool coroutine_frames;

        std::thread thread;
        std::deque<detail::CompletionStateBase*> deferred_completed;
        std::deque<detail::CompletionStateBase*> deferred_blocking_completed;

        std::atomic_flag poller_update = ATOMIC_FLAG_INIT;
        std::vector<std::unique_ptr<const PollerList>> poller_generations;
        std::atomic<const PollerList*> pollers{nullptr};
        std::atomic<bool> poller_generations_dirty{false};
        // Shared with registrations so reset remains safe when a poller object
        // outlives a Runtime whose worker has already stopped.
        std::shared_ptr<PollRegistration::State::Quiescence> quiescence{
            std::make_shared<PollRegistration::State::Quiescence>()};

        Worker()
            : work_node_pool(
                  std::make_unique<WorkNode[]>(kWorkNodePoolSize)),
              remote_work_node_pool(
                  std::make_unique<WorkNode[]>(kRemoteWorkNodePoolSize)) {
            for (std::size_t index = 0; index < kWorkNodePoolSize; ++index) {
                work_node_pool[index].pooled = true;
                work_node_pool[index].next =
                    index + 1 < kWorkNodePoolSize
                        ? &work_node_pool[index + 1]
                        : nullptr;
            }
            free_work_nodes = work_node_pool.get();
            for (std::size_t index = 0; index < kRemoteWorkNodePoolSize;
                 ++index) {
                remote_work_node_pool[index].pooled = true;
                remote_work_node_pool[index].remote_pooled = true;
            }
            auto initial = std::make_unique<const PollerList>();
            pollers.store(initial.get(), std::memory_order_relaxed);
            poller_generations.push_back(std::move(initial));
        }

        [[nodiscard]] WorkNode* acquire_work_node(bool owner_local) noexcept {
            if (owner_local && free_work_nodes != nullptr) {
                WorkNode* node = free_work_nodes;
                free_work_nodes = node->next;
                node->next = nullptr;
                return node;
            }
            if (!owner_local) {
                const std::size_t start = remote_work_node_cursor.fetch_add(
                    1, std::memory_order_relaxed);
                for (std::size_t probe = 0;
                     probe < kRemoteWorkNodePoolSize; ++probe) {
                    WorkNode& node = remote_work_node_pool[
                        (start + probe) & (kRemoteWorkNodePoolSize - 1U)];
                    bool expected = false;
                    if (node.remote_claimed.compare_exchange_strong(
                            expected, true, std::memory_order_acquire,
                            std::memory_order_relaxed)) {
                        node.next = nullptr;
                        return &node;
                    }
                }
            }
            return new (std::nothrow) WorkNode();
        }

        void release_work_node(WorkNode* node) noexcept {
            if (node == nullptr) {
                return;
            }
            node->item = {};
            if (!node->pooled) {
                delete node;
                return;
            }
            if (node->remote_pooled) {
                node->next = nullptr;
                node->remote_claimed.store(false, std::memory_order_release);
                return;
            }
            node->next = free_work_nodes;
            free_work_nodes = node;
        }

        ~Worker() {
            WorkNode* node = inbox.take_all();
            while (node != nullptr) {
                WorkNode* next = node->next;
                if (!node->pooled) {
                    delete node;
                }
                node = next;
            }
            node = local_head;
            while (node != nullptr) {
                WorkNode* next = node->next;
                if (!node->pooled) {
                    delete node;
                }
                node = next;
            }
        }
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
    std::atomic<std::size_t> root_submitters{0};
    std::atomic<std::size_t> scheduled_items{0};
    std::atomic<std::size_t> round_robin{0};
    std::atomic<std::size_t> blocking_waiters{0};
    std::atomic<std::size_t> blocking_completions{0};
    std::atomic<bool> blocking_spin_disabled{false};

    std::atomic<bool> scheduling_closed{false};

    std::atomic_flag roots_update = ATOMIC_FLAG_INIT;
    RootRegistry roots;

    PinRegistry pinned_coroutines;

    std::atomic<bool> start_gate{false};
    std::atomic<bool> abort_start{false};
    std::atomic<bool> join_started{false};

    void wake_worker(std::size_t worker_index) noexcept {
        auto& epoch = workers[worker_index]->quiescence->wake_epoch;
        epoch.fetch_add(1, std::memory_order_release);
        futex_wake(epoch, 1);
    }

    void wake_all() noexcept {
        for (std::size_t worker_index = 0; worker_index < workers.size();
             ++worker_index) {
            wake_worker(worker_index);
        }
    }

    void drain_inbox(std::size_t worker_index) noexcept {
        auto& worker = *workers[worker_index];
        // Remote publications are sparse compared with the busy-poll rate.
        // Avoid an unconditional atomic exchange (and cache-line ownership)
        // on every empty iteration.  wake_epoch still closes the load/park
        // race when a producer publishes immediately after this check.
        if (worker.inbox.empty()) {
            return;
        }
        WorkNode* fifo = worker.inbox.drain();
        if (fifo == nullptr) {
            return;
        }

        WorkNode* tail = fifo;
        while (tail->next != nullptr) {
            tail = tail->next;
        }
        if (worker.local_tail != nullptr) {
            worker.local_tail->next = fifo;
        } else {
            worker.local_head = fifo;
        }
        worker.local_tail = tail;
    }

    [[nodiscard]] bool pop_local(std::size_t worker_index,
                                 WorkItem& result) noexcept {
        drain_inbox(worker_index);
        auto& worker = *workers[worker_index];
        WorkNode* node = worker.local_head;
        if (node == nullptr) {
            return false;
        }
        worker.local_head = node->next;
        if (worker.local_head == nullptr) {
            worker.local_tail = nullptr;
        }
        result = node->item;
        worker.release_work_node(node);
        return true;
    }

    [[nodiscard]] bool close_scheduling_if_drained() noexcept {
        if (scheduling_closed.load(std::memory_order_acquire)) {
            return true;
        }
        if (!stop_requested.load(std::memory_order_acquire) ||
            root_submitters.load(std::memory_order_acquire) != 0 ||
            pending_roots.load(std::memory_order_acquire) != 0 ||
            scheduled_items.load(std::memory_order_seq_cst) != 0) {
            return false;
        }
        bool expected = false;
        if (!scheduling_closed.compare_exchange_strong(
                expected, true, std::memory_order_seq_cst,
                std::memory_order_seq_cst)) {
            return expected;
        }
        if (pending_roots.load(std::memory_order_acquire) != 0 ||
            root_submitters.load(std::memory_order_acquire) != 0 ||
            scheduled_items.load(std::memory_order_seq_cst) != 0) {
            scheduling_closed.store(false, std::memory_order_seq_cst);
            return false;
        }
        // Worker 0 is the only closer. Other workers may already be parked
        // after consuming request_stop()'s wake, so publish the terminal
        // decision with a fresh wake for every worker.
        wake_all();
        return true;
    }

    void publish_deferred_completions(std::size_t worker_index) noexcept {
        auto& worker = *workers[worker_index];
        auto& deferred = worker.deferred_completed;
        if (!deferred.empty()) {
            {
                detail::AtomicFlagGuard guard(roots_update);
                for (auto* completed : deferred) {
                    roots.erase(completed);
                }
            }
            deferred.clear();
        }

        auto& blocking = worker.deferred_blocking_completed;
        for (auto* completed : blocking) {
            // resume() has returned, so the root frame is no longer executing.
            // Finish every Runtime-side access before publishing readiness;
            // the external waiter owns the stack completion after that point.
            completed->destroy_blocking_root();
            pending_roots.fetch_sub(1, std::memory_order_acq_rel);
            wake_all();
            completed->publish_blocking_ready();
        }
        blocking.clear();
    }

    [[nodiscard]] PollState poll_services(std::size_t worker_index) noexcept {
        PollState aggregate = PollState::idle;
        const auto* pollers = workers[worker_index]->pollers.load(
            std::memory_order_acquire);
        for (const auto& poller : *pollers) {
            if ((poller->lifecycle.load(std::memory_order_acquire) &
                 PollRegistration::State::kActive) == 0U) {
                continue;
            }
            const PollState state = poller->function(poller->context);
            if (state == PollState::progress) {
                aggregate = PollState::progress;
            } else if (state == PollState::busy &&
                       aggregate == PollState::idle) {
                aggregate = PollState::busy;
            }
        }
        return aggregate;
    }

    void reclaim_poller_generations(std::size_t worker_index) noexcept {
        auto& worker = *workers[worker_index];
        // Registration changes are rare, while this runs on every busy-poll
        // iteration.  Keep the steady-state path read-only so it does not
        // acquire exclusive ownership of the cache line on every pass.
        if (!worker.poller_generations_dirty.load(std::memory_order_acquire) ||
            !worker.poller_generations_dirty.exchange(
                false, std::memory_order_acq_rel)) {
            return;
        }
        detail::AtomicFlagGuard guard(worker.poller_update);
        const auto* current = worker.pollers.load(std::memory_order_acquire);
        auto& generations = worker.poller_generations;
        generations.erase(
            std::remove_if(generations.begin(), generations.end(),
                           [current](const auto& generation) {
                               return generation.get() != current;
                           }),
            generations.end());
    }

    void worker_loop(std::size_t worker_index) noexcept {
        while (!start_gate.load(std::memory_order_acquire)) {
            start_gate.wait(false, std::memory_order_acquire);
        }
        if (abort_start.load(std::memory_order_acquire)) {
            workers[worker_index]->quiescence->exited.store(
                true, std::memory_order_release);
            workers[worker_index]->quiescence->exited.notify_all();
            return;
        }

        tls_runtime = runtime;
        tls_worker = worker_index;
        tls_pin_depth = 0;
        tls_owner = false;
        tls_frame_pool = &workers[worker_index]->coroutine_frames;

        std::size_t spins = 0;
        std::uint64_t poll_epoch = 0;
        for (;;) {
            auto& worker = *workers[worker_index];
            const std::uint32_t observed_wake =
                worker.quiescence->wake_epoch.load(std::memory_order_acquire);
            const PollState service_state = poll_services(worker_index);
            worker.quiescence->poll_epoch.store(++poll_epoch,
                                                 std::memory_order_release);
            // Poller lists are immutable generations. Only their owning worker
            // reads the published raw pointer, so the owner can reclaim every
            // older generation immediately after an iteration. Publishers
            // serialize with this short section and never free generations.
            reclaim_poller_generations(worker_index);
            WorkItem item;
            if (pop_local(worker_index, item)) {
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
                continue;
            }

            if (service_state == PollState::progress) {
                spins = 0;
                continue;
            }

            if ((worker_index == 0 && close_scheduling_if_drained()) ||
                (worker_index != 0 &&
                 scheduling_closed.load(std::memory_order_acquire))) {
                break;
            }

            if (spins++ < options.spin_before_park) {
                _mm_pause();
                continue;
            }

            if (service_state == PollState::busy) {
                spins = 0;
                _mm_pause();
                continue;
            }

            spins = 0;
            futex_wait_for(worker.quiescence->wake_epoch, observed_wake);
        }

        tls_pin_depth = 0;
        tls_owner = false;
        tls_frame_pool = nullptr;
        tls_worker = detail::no_worker;
        tls_runtime = nullptr;
        workers[worker_index]->quiescence->exited.store(
            true, std::memory_order_release);
        workers[worker_index]->quiescence->exited.notify_all();
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

    impl_->abort_start.store(static_cast<bool>(start_error),
                             std::memory_order_release);
    impl_->start_gate.store(true, std::memory_order_release);
    impl_->start_gate.notify_all();

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
    impl_->root_submitters.fetch_add(1, std::memory_order_acq_rel);
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        impl_->root_submitters.fetch_sub(1, std::memory_order_release);
        return Result<void>::failure(Errc::runtime_stopping);
    }

    try {
        detail::AtomicFlagGuard guard(impl_->roots_update);
        const bool inserted = impl_->roots.emplace(state.get(), state).second;
        if (!inserted) {
            std::terminate();
        }
    } catch (const std::bad_alloc&) {
        impl_->root_submitters.fetch_sub(1, std::memory_order_release);
        return Result<void>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        std::terminate();
    }
    impl_->pending_roots.fetch_add(1, std::memory_order_release);
    impl_->root_submitters.fetch_sub(1, std::memory_order_release);
    const std::size_t worker =
        impl_->round_robin.fetch_add(1, std::memory_order_relaxed) %
        impl_->workers.size();

    if (!schedule_internal(coroutine, worker, 0, false)) {
        {
            detail::AtomicFlagGuard guard(impl_->roots_update);
            impl_->roots.erase(state.get());
        }
        impl_->pending_roots.fetch_sub(1, std::memory_order_release);
        return Result<void>::failure(Errc::runtime_stopping);
    }
    return Result<void>::success();
}

Result<void> Runtime::submit_blocking_root(
    std::coroutine_handle<> coroutine) noexcept {
    impl_->root_submitters.fetch_add(1, std::memory_order_acq_rel);
    if (!impl_->accepting.load(std::memory_order_acquire)) {
        impl_->root_submitters.fetch_sub(1, std::memory_order_release);
        return Result<void>::failure(Errc::runtime_stopping);
    }

    impl_->pending_roots.fetch_add(1, std::memory_order_release);
    impl_->root_submitters.fetch_sub(1, std::memory_order_release);
    const std::size_t worker =
        impl_->round_robin.fetch_add(1, std::memory_order_relaxed) %
        impl_->workers.size();
    if (!schedule_internal(coroutine, worker, 0, false)) {
        impl_->pending_roots.fetch_sub(1, std::memory_order_release);
        impl_->wake_all();
        return Result<void>::failure(Errc::runtime_stopping);
    }
    return Result<void>::success();
}

void Runtime::request_stop() noexcept {
    impl_->accepting.store(false, std::memory_order_release);
    impl_->stop_requested.store(true, std::memory_order_release);
    impl_->wake_all();
}

Result<void> Runtime::join() noexcept {
    if (current() == this) {
        return Result<void>::failure(Errc::join_from_worker);
    }

    if (impl_->joined.load(std::memory_order_acquire)) {
        return Result<void>::success();
    }
    bool expected = false;
    if (!impl_->join_started.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel,
            std::memory_order_acquire)) {
        while (!impl_->joined.load(std::memory_order_acquire)) {
            impl_->joined.wait(false, std::memory_order_acquire);
        }
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
    impl_->joined.notify_all();
    return Result<void>::success();
}

std::size_t Runtime::worker_count() const noexcept {
    return impl_->workers.size();
}

Result<unsigned> Runtime::worker_cpu(std::size_t worker) const noexcept {
    if (worker >= impl_->cpus.size()) {
        return Result<unsigned>::failure(Errc::invalid_worker);
    }
    return Result<unsigned>::success(impl_->cpus[worker]);
}

Result<Runtime::PollRegistration> Runtime::register_poller(
    std::size_t worker, PollFunction function, void* context) noexcept {
    if (worker >= impl_->workers.size()) {
        return Result<PollRegistration>::failure(Errc::invalid_worker);
    }
    if (function == nullptr) {
        return Result<PollRegistration>::failure(
            std::make_error_code(std::errc::invalid_argument));
    }
    if (!impl_->running.load(std::memory_order_acquire) ||
        impl_->joined.load(std::memory_order_acquire)) {
        return Result<PollRegistration>::failure(Errc::runtime_stopping);
    }

    try {
        auto state = std::make_shared<PollRegistration::State>();
        state->runtime = this;
        state->worker = worker;
        state->function = function;
        state->context = context;

        auto& target = *impl_->workers[worker];
        state->quiescence = target.quiescence;
        {
            detail::AtomicFlagGuard guard(target.poller_update);
            const auto* current = target.pollers.load(std::memory_order_acquire);
            auto updated = std::make_unique<Impl::PollerList>();
            updated->reserve(current->size() + 1);
            for (const auto& existing : *current) {
                if ((existing->lifecycle.load(std::memory_order_acquire) &
                     PollRegistration::State::kActive) != 0U) {
                    updated->push_back(existing);
                }
            }
            updated->push_back(state);
            const auto* published = updated.get();
            target.poller_generations.push_back(std::move(updated));
            target.pollers.store(published, std::memory_order_release);
            target.poller_generations_dirty.store(true,
                                                  std::memory_order_release);
        }
        impl_->wake_worker(worker);
        return Result<PollRegistration>::success(
            PollRegistration(std::move(state)));
    } catch (const std::bad_alloc&) {
        return Result<PollRegistration>::failure(
            std::make_error_code(std::errc::not_enough_memory));
    } catch (...) {
        std::terminate();
    }
}

std::size_t Runtime::owner_for(std::uint64_t key) const noexcept {
    // A fixed integer hash avoids depending on an implementation-defined
    // std::hash salt while still spreading sequential inode IDs.
    return static_cast<std::size_t>(
        detail::fmix64(key) % impl_->workers.size());
}

RuntimePollerStats Runtime::poller_stats() const noexcept {
    RuntimePollerStats stats;
    for (const auto& worker_ptr : impl_->workers) {
        auto& worker = *worker_ptr;
        detail::AtomicFlagGuard guard(worker.poller_update);
        stats.generations += worker.poller_generations.size();
        const auto* pollers = worker.pollers.load(std::memory_order_acquire);
        for (const auto& poller : *pollers) {
            if ((poller->lifecycle.load(std::memory_order_acquire) &
                 PollRegistration::State::kActive) != 0U) {
                ++stats.active_pollers;
            }
        }
    }
    return stats;
}

bool Runtime::notify(std::size_t worker) noexcept {
    if (worker >= impl_->workers.size() ||
        !impl_->running.load(std::memory_order_acquire) ||
        impl_->joined.load(std::memory_order_acquire)) {
        return false;
    }
    // Each worker has its own futex epoch, so an owner-local publication wakes
    // exactly the worker that can consume it.
    impl_->wake_worker(worker);
    return true;
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
    const detail::ResumeTarget pinned =
        impl_->pinned_coroutines.find(this, coroutine.address());

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
    if (impl_->scheduling_closed.load(std::memory_order_acquire) ||
        !impl_->running.load(std::memory_order_acquire) ||
        impl_->joined.load(std::memory_order_acquire)) {
        return false;
    }

    auto& target = *impl_->workers[worker];
    const bool owner_local = tls_runtime == this && tls_worker == worker;
    auto* node = target.acquire_work_node(owner_local);
    if (node == nullptr) {
        return false;
    }
    node->item = WorkItem{coroutine, pin_depth, owner || pin_depth != 0};

    // Increment before the second admission check. This pairs with the final
    // close CAS: either the item is visible in scheduled_items, or it observes
    // scheduling_closed and rolls itself back before publishing to an inbox.
    impl_->scheduled_items.fetch_add(1, std::memory_order_seq_cst);
    if (impl_->scheduling_closed.load(std::memory_order_seq_cst) ||
        !impl_->running.load(std::memory_order_acquire) ||
        impl_->joined.load(std::memory_order_acquire)) {
        impl_->scheduled_items.fetch_sub(1, std::memory_order_seq_cst);
        target.release_work_node(node);
        return false;
    }

    if (tls_runtime == this && tls_worker == worker) {
        if (target.local_tail != nullptr) {
            target.local_tail->next = node;
        } else {
            target.local_head = node;
        }
        target.local_tail = node;
    } else {
        target.inbox.push(*node);
    }
    impl_->wake_worker(worker);
    return true;
}

void Runtime::register_pin(std::coroutine_handle<> coroutine,
                           detail::ResumeTarget target) noexcept {
    if (!coroutine || target.runtime != this ||
        target.worker >= impl_->workers.size() || target.pin_depth == 0) {
        std::terminate();
    }
    impl_->pinned_coroutines.set(coroutine.address(), target);
}

void Runtime::transfer_pin(std::coroutine_handle<> coroutine,
                           std::coroutine_handle<> continuation) noexcept {
    if (!coroutine || !continuation || tls_runtime != this ||
        tls_worker >= impl_->workers.size()) {
        std::terminate();
    }
    impl_->pinned_coroutines.erase(coroutine.address());
    if (tls_pin_depth == 0) {
        impl_->pinned_coroutines.erase(continuation.address());
    } else {
        impl_->pinned_coroutines.set(
            continuation.address(),
            {this, tls_worker, tls_pin_depth, tls_owner});
    }
}

void Runtime::unregister_pin(std::coroutine_handle<> coroutine) noexcept {
    if (!coroutine) {
        std::terminate();
    }
    if (tls_pin_depth == 0) {
        impl_->pinned_coroutines.erase(coroutine.address());
    } else {
        impl_->pinned_coroutines.set(
            coroutine.address(),
            {this, tls_worker, tls_pin_depth, tls_owner});
    }
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

void Runtime::on_blocking_root_completed(
    detail::CompletionStateBase* state) noexcept {
    if (tls_runtime != this || tls_worker >= impl_->workers.size() ||
        state == nullptr) {
        std::terminate();
    }
    impl_->workers[tls_worker]->deferred_blocking_completed.push_back(state);
}

detail::UnobservedErrorHandler Runtime::unobserved_error_handler() const {
    return impl_->options.on_unobserved_error;
}

std::size_t Runtime::begin_blocking_wait() noexcept {
    const std::size_t waiters =
        impl_->blocking_waiters.fetch_add(1, std::memory_order_acq_rel) + 1;
    if (waiters > impl_->options.blocking_spin_limit) {
        impl_->blocking_spin_disabled.store(true, std::memory_order_release);
        return 0;
    }
    if (impl_->blocking_spin_disabled.load(std::memory_order_acquire) ||
        impl_->blocking_completions.load(std::memory_order_relaxed) <
            impl_->options.blocking_spin_warmup) {
        return 0;
    }
    return impl_->options.blocking_spin_count;
}

void Runtime::end_blocking_wait() noexcept {
    const std::size_t previous =
        impl_->blocking_waiters.fetch_sub(1, std::memory_order_acq_rel);
    if (previous == 0) {
        std::terminate();
    }
    impl_->blocking_completions.fetch_add(1, std::memory_order_relaxed);
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

void* allocate_coroutine_frame(std::size_t size) {
    return tls_frame_pool != nullptr
               ? tls_frame_pool->allocate(size)
               : CoroutineFramePool::allocate_heap(size);
}

void deallocate_coroutine_frame(void* frame) noexcept {
    CoroutineFramePool::release(frame);
}

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

void blocking_root_completed(Runtime* runtime,
                             CompletionStateBase* state) noexcept {
    if (runtime == nullptr || state == nullptr) {
        std::terminate();
    }
    runtime->on_blocking_root_completed(state);
}

} // namespace detail
} // namespace orchfs::async
