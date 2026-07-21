#include "orchfs/async/range_arbiter.hpp"

#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <deque>
#include <immintrin.h>
#include <limits>
#include <new>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace detail {

enum class RangeWaitPhase : std::uint8_t {
    installing,
    parked,
    granted,
};

enum class RangeOperation : std::uint8_t {
    acquire,
    release,
};

struct RangeRequest {
    std::uint64_t first_block{0};
    std::uint64_t last_block{0};
    RangeMode mode{RangeMode::read};
    std::coroutine_handle<> coroutine{};
    ResumeTarget resume_target{};
    std::atomic<RangeWaitPhase> phase{RangeWaitPhase::installing};
    std::atomic<bool> granted{false};
    std::atomic<int> error{0};
    bool pin_applied{false};

    RangeOperation operation{RangeOperation::acquire};
    RangeRequest* next{};
    std::shared_ptr<RangeRequest> queued_reference;
};

[[nodiscard]] bool overlaps(const RangeRequest& left,
                            const RangeRequest& right) noexcept {
    return left.first_block <= right.last_block &&
           right.first_block <= left.last_block;
}

[[nodiscard]] bool conflicts(const RangeRequest& left,
                             const RangeRequest& right) noexcept {
    return overlaps(left, right) &&
           (left.mode == RangeMode::write || right.mode == RangeMode::write);
}

struct RangeState {
    enum class AcquireResult : std::uint8_t {
        granted,
        waiting,
        failed,
    };

    ~RangeState() {
        registration.reset();
        if (inbox.load(std::memory_order_acquire) != nullptr ||
            !active.empty() || !waiting.empty()) {
            std::terminate();
        }
    }

    [[nodiscard]] std::error_code ensure_owner(Runtime& runtime,
                                                std::size_t worker) noexcept {
        while (owner_update.test_and_set(std::memory_order_acquire)) {
            _mm_pause();
        }
        std::error_code error;
        if (owner_runtime == nullptr) {
            owner_runtime = &runtime;
            owner_worker = worker;
        } else if (owner_runtime != &runtime) {
            error = make_error_code(Errc::wrong_runtime);
        }
        owner_update.clear(std::memory_order_release);
        return error;
    }

    [[nodiscard]] std::error_code ensure_poller() noexcept {
        while (owner_update.test_and_set(std::memory_order_acquire)) {
            _mm_pause();
        }
        std::error_code error;
        if (!registration) {
            auto registered = owner_runtime->register_poller(
                owner_worker, &RangeState::poll, this);
            if (!registered) {
                error = registered.error();
            } else {
                registration = std::move(registered).value();
            }
        }
        owner_update.clear(std::memory_order_release);
        return error;
    }

    [[nodiscard]] bool on_owner() const noexcept {
        return Runtime::current() == owner_runtime &&
               Runtime::current_worker() == owner_worker;
    }

    void enqueue(RangeRequest& request, RangeOperation operation) noexcept {
        request.operation = operation;
        RangeRequest* head = inbox.load(std::memory_order_relaxed);
        do {
            request.next = head;
        } while (!inbox.compare_exchange_weak(
            head, &request, std::memory_order_release,
            std::memory_order_relaxed));
        if (!owner_runtime->notify(owner_worker)) {
            std::terminate();
        }
    }

    [[nodiscard]] AcquireResult acquire_on_owner(
        const std::shared_ptr<RangeRequest>& request) noexcept {
        if (!on_owner()) {
            std::terminate();
        }
        try {
            if (can_grant_immediately(*request)) {
                active.push_back(request);
                active_size.store(active.size(), std::memory_order_release);
                wake_granted(request, false, nullptr);
                return AcquireResult::granted;
            }
            waiting.push_back(request);
            waiting_size.store(waiting.size(), std::memory_order_release);
            return AcquireResult::waiting;
        } catch (const std::bad_alloc&) {
            request->error.store(ENOMEM, std::memory_order_release);
            wake_completed(*request);
            return AcquireResult::failed;
        } catch (...) {
            std::terminate();
        }
    }

    [[nodiscard]] std::shared_ptr<RangeRequest> release_on_owner(
        const std::shared_ptr<RangeRequest>& request,
        ResumeTarget releaser_target,
        std::error_code& error) noexcept {
        if (!on_owner()) {
            std::terminate();
        }
        auto found = std::find(active.begin(), active.end(), request);
        if (found == active.end()) {
            error = make_error_code(Errc::invalid_handle);
            return {};
        }
        active.erase(found);
        active_size.store(active.size(), std::memory_order_release);
        return grant_waiters(true, releaser_target);
    }

    std::atomic<std::size_t> active_size{0};
    std::atomic<std::size_t> waiting_size{0};

private:
    static Runtime::PollState poll(void* context) noexcept {
        return static_cast<RangeState*>(context)->poll_once();
    }

    Runtime::PollState poll_once() noexcept {
        RangeRequest* stack = inbox.exchange(nullptr, std::memory_order_acquire);
        if (stack == nullptr) {
            return Runtime::PollState::idle;
        }
        RangeRequest* fifo = nullptr;
        while (stack != nullptr) {
            RangeRequest* next = stack->next;
            stack->next = fifo;
            fifo = stack;
            stack = next;
        }
        while (fifo != nullptr) {
            RangeRequest* request = fifo;
            fifo = fifo->next;
            request->next = nullptr;
            if (request->operation == RangeOperation::acquire) {
                auto reference = std::move(request->queued_reference);
                if (!reference || reference.get() != request) {
                    std::terminate();
                }
                (void)acquire_on_owner(reference);
            } else {
                release_from_queue(*request);
            }
        }
        return Runtime::PollState::progress;
    }

    [[nodiscard]] bool conflicts_with_active(
        const RangeRequest& request) const noexcept {
        return std::any_of(active.begin(), active.end(),
                           [&](const auto& other) {
                               return conflicts(request, *other);
                           });
    }

    [[nodiscard]] bool can_grant_immediately(
        const RangeRequest& request) const noexcept {
        if (conflicts_with_active(request)) {
            return false;
        }
        return std::none_of(waiting.begin(), waiting.end(),
                            [&](const auto& waiter) {
                                return conflicts(request, *waiter);
                            });
    }

    static void wake_completed(RangeRequest& request) noexcept {
        const auto phase = request.phase.exchange(
            RangeWaitPhase::granted, std::memory_order_acq_rel);
        if (phase == RangeWaitPhase::installing) {
            return;
        }
        if (phase != RangeWaitPhase::parked) {
            std::terminate();
        }
        schedule_resume(request.resume_target, request.coroutine,
                        request.resume_target.pin_depth != 0);
    }

    void wake_granted(const std::shared_ptr<RangeRequest>& request,
                      bool allow_direct,
                      std::shared_ptr<RangeRequest>* direct) noexcept {
        request->granted.store(true, std::memory_order_release);
        const auto phase = request->phase.exchange(
            RangeWaitPhase::granted, std::memory_order_acq_rel);
        if (phase == RangeWaitPhase::installing) {
            return;
        }
        if (phase != RangeWaitPhase::parked) {
            std::terminate();
        }

        auto target = request->resume_target;
        if (target.pin_depth == 0 && !target.owner) {
            target.runtime = owner_runtime;
            target.worker = owner_worker;
            target.owner = true;
        }
        ++target.pin_depth;
        request->resume_target = target;
        request->pin_applied = true;
        register_pin(request->coroutine, target);

        if (allow_direct && direct != nullptr && !*direct &&
            target.runtime == owner_runtime && target.worker == owner_worker) {
            *direct = request;
            return;
        }
        schedule_resume(target, request->coroutine, true);
    }

    [[nodiscard]] std::shared_ptr<RangeRequest> grant_waiters(
        bool allow_direct, ResumeTarget releaser_target) noexcept {
        std::shared_ptr<RangeRequest> direct;
        try {
            active.reserve(active.size() + waiting.size());
            for (auto iterator = waiting.begin(); iterator != waiting.end();) {
                const auto& request = *iterator;
                bool blocked = conflicts_with_active(*request);
                for (auto earlier = waiting.begin();
                     !blocked && earlier != iterator; ++earlier) {
                    blocked = conflicts(*request, **earlier);
                }
                if (blocked) {
                    ++iterator;
                    continue;
                }
                auto granted = request;
                active.push_back(granted);
                iterator = waiting.erase(iterator);
                wake_granted(granted, allow_direct, &direct);
            }
        } catch (...) {
            // release() has consumed the permit; rollback cannot safely
            // recreate the prior ownership graph.
            std::terminate();
        }
        active_size.store(active.size(), std::memory_order_release);
        waiting_size.store(waiting.size(), std::memory_order_release);
        (void)releaser_target;
        return direct;
    }

    void release_from_queue(RangeRequest& request) noexcept {
        auto found = std::find_if(active.begin(), active.end(),
                                  [&](const auto& candidate) {
                                      return candidate.get() == &request;
                                  });
        if (found == active.end()) {
            request.error.store(
                make_error_code(Errc::invalid_handle).value(),
                std::memory_order_release);
        } else {
            active.erase(found);
            active_size.store(active.size(), std::memory_order_release);
            (void)grant_waiters(false, {});
        }
        wake_completed(request);
    }

    std::atomic_flag owner_update = ATOMIC_FLAG_INIT;
    Runtime* owner_runtime{};
    std::size_t owner_worker{no_worker};
    Runtime::PollRegistration registration;

    std::atomic<RangeRequest*> inbox{nullptr};
    std::vector<std::shared_ptr<RangeRequest>> active;
    std::deque<std::shared_ptr<RangeRequest>> waiting;
};

} // namespace detail

RangeAcquireAwaiter::RangeAcquireAwaiter(
    std::shared_ptr<detail::RangeState> state,
    std::uint64_t offset,
    std::uint64_t length,
    RangeMode mode) noexcept
    : state_(std::move(state)) {
    if (!state_ || length == 0 ||
        offset > std::numeric_limits<std::uint64_t>::max() - (length - 1)) {
        error_ = make_error_code(Errc::invalid_range);
        return;
    }
    try {
        request_ = std::make_shared<detail::RangeRequest>();
    } catch (const std::bad_alloc&) {
        error_ = std::make_error_code(std::errc::not_enough_memory);
        return;
    } catch (...) {
        std::terminate();
    }
    request_->first_block = offset / RangeArbiter::granularity;
    request_->last_block = (offset + length - 1) / RangeArbiter::granularity;
    request_->mode = mode;
}

bool RangeAcquireAwaiter::await_suspend(
    std::coroutine_handle<> coroutine) noexcept {
    if (error_) {
        return false;
    }
    const auto target = detail::current_resume_target();
    if (target.runtime == nullptr || target.worker == detail::no_worker) {
        error_ = make_error_code(Errc::not_in_runtime);
        return false;
    }
    error_ = state_->ensure_owner(*target.runtime, target.worker);
    if (error_) {
        return false;
    }

    request_->coroutine = coroutine;
    request_->resume_target = target;
    if (state_->on_owner()) {
        const auto result = state_->acquire_on_owner(request_);
        if (result != detail::RangeState::AcquireResult::waiting) {
            return false;
        }
    } else {
        error_ = state_->ensure_poller();
        if (error_) {
            return false;
        }
        request_->queued_reference = request_;
        state_->enqueue(*request_, detail::RangeOperation::acquire);
    }

    const auto phase = request_->phase.exchange(
        detail::RangeWaitPhase::parked, std::memory_order_acq_rel);
    if (phase == detail::RangeWaitPhase::granted) {
        return false;
    }
    if (phase != detail::RangeWaitPhase::installing) {
        std::terminate();
    }
    return true;
}

Result<RangePermit> RangeAcquireAwaiter::await_resume() noexcept {
    if (error_) {
        return Result<RangePermit>::failure(error_);
    }
    if (request_) {
        const int request_error =
            request_->error.load(std::memory_order_acquire);
        if (request_error != 0) {
            return Result<RangePermit>::failure(
                std::error_code(request_error, std::generic_category()));
        }
    }
    if (!request_ || !request_->granted.load(std::memory_order_acquire)) {
        return Result<RangePermit>::failure(Errc::invalid_handle);
    }
    if (!request_->pin_applied) {
        detail::pin_current(request_->coroutine);
        request_->pin_applied = true;
    }
    return Result<RangePermit>::success(RangePermit(state_, request_));
}

RangePermit::RangePermit(RangePermit&& other) noexcept
    : state_(std::move(other.state_)), request_(std::move(other.request_)),
      owns_(std::exchange(other.owns_, false)) {}

RangePermit& RangePermit::operator=(RangePermit&& other) noexcept {
    if (this == &other) {
        return *this;
    }
    if (owns_) {
        std::terminate();
    }
    state_ = std::move(other.state_);
    request_ = std::move(other.request_);
    owns_ = std::exchange(other.owns_, false);
    return *this;
}

RangePermit::~RangePermit() {
    if (owns_) {
        std::terminate();
    }
}

std::uint64_t RangePermit::first_block() const noexcept {
    return request_ ? request_->first_block : 0;
}

std::uint64_t RangePermit::block_count() const noexcept {
    return request_ ? request_->last_block - request_->first_block + 1 : 0;
}

RangeMode RangePermit::mode() const noexcept {
    return request_ ? request_->mode : RangeMode::read;
}

RangePermit::ReleaseAwaiter::ReleaseAwaiter(ReleaseAwaiter&& other) noexcept
    : state_(std::move(other.state_)), request_(std::move(other.request_)),
      error_(other.error_), armed_(std::exchange(other.armed_, false)) {}

RangePermit::ReleaseAwaiter::~ReleaseAwaiter() {
    if (armed_) {
        std::terminate();
    }
}

RangePermit::ReleaseAwaiter RangePermit::release() noexcept {
    if (!owns_ || !state_ || !request_) {
        return ReleaseAwaiter({}, {}, make_error_code(Errc::invalid_handle));
    }
    owns_ = false;
    return ReleaseAwaiter(std::move(state_), std::move(request_), {});
}

std::coroutine_handle<> RangePermit::ReleaseAwaiter::await_suspend(
    std::coroutine_handle<> releaser) noexcept {
    armed_ = false;
    if (error_ || !state_ || !request_) {
        if (!error_) {
            error_ = make_error_code(Errc::invalid_handle);
        }
        return releaser;
    }

    if (!state_->on_owner()) {
        error_ = state_->ensure_poller();
        if (error_) {
            return releaser;
        }
    }

    detail::unpin_current(releaser);
    const auto releaser_target = detail::current_resume_target();
    request_->coroutine = releaser;
    request_->resume_target = releaser_target;
    request_->phase.store(detail::RangeWaitPhase::installing,
                          std::memory_order_release);
    request_->error.store(0, std::memory_order_release);

    if (state_->on_owner()) {
        auto direct = state_->release_on_owner(
            request_, releaser_target, error_);
        if (error_ || !direct) {
            return releaser;
        }
        detail::schedule_resume(releaser_target, releaser,
                                releaser_target.pin_depth != 0);
        detail::prepare_symmetric_resume(direct->resume_target);
        return direct->coroutine;
    }

    state_->enqueue(*request_, detail::RangeOperation::release);
    const auto phase = request_->phase.exchange(
        detail::RangeWaitPhase::parked, std::memory_order_acq_rel);
    if (phase == detail::RangeWaitPhase::granted) {
        return releaser;
    }
    if (phase != detail::RangeWaitPhase::installing) {
        std::terminate();
    }
    return std::noop_coroutine();
}

Result<void> RangePermit::ReleaseAwaiter::await_resume() const noexcept {
    if (error_) {
        return Result<void>::failure(error_);
    }
    if (request_) {
        const int request_error =
            request_->error.load(std::memory_order_acquire);
        if (request_error != 0) {
            return Result<void>::failure(
                std::error_code(request_error, std::generic_category()));
        }
    }
    return Result<void>::success();
}

RangeArbiter::RangeArbiter()
    : state_(std::make_shared<detail::RangeState>()) {}

RangeAcquireAwaiter RangeArbiter::acquire(std::uint64_t offset,
                                          std::uint64_t length,
                                          RangeMode mode) noexcept {
    return RangeAcquireAwaiter(state_, offset, length, mode);
}

std::size_t RangeArbiter::active_count() const noexcept {
    return state_ ? state_->active_size.load(std::memory_order_acquire) : 0;
}

std::size_t RangeArbiter::waiting_count() const noexcept {
    return state_ ? state_->waiting_size.load(std::memory_order_acquire) : 0;
}

} // namespace orchfs::async
