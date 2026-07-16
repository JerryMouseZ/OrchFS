#include "orchfs/async/range_arbiter.hpp"

#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <deque>
#include <limits>
#include <mutex>
#include <new>
#include <utility>
#include <vector>

namespace orchfs::async {
namespace detail {

struct RangeRequest {
    std::uint64_t first_block{0};
    std::uint64_t last_block{0};
    RangeMode mode{RangeMode::read};
    std::coroutine_handle<> coroutine{};
    ResumeTarget resume_target{};
    bool granted{false};
    bool pin_applied{false};
};

struct RangeState {
    mutable std::mutex mutex;
    std::vector<std::shared_ptr<RangeRequest>> active;
    std::deque<std::shared_ptr<RangeRequest>> waiting;
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

[[nodiscard]] bool conflicts_with(
    const RangeRequest& request,
    const std::vector<std::shared_ptr<RangeRequest>>& others) noexcept {
    return std::any_of(others.begin(), others.end(), [&](const auto& other) {
        return conflicts(request, *other);
    });
}

[[nodiscard]] bool can_grant_immediately(const RangeState& state,
                                         const RangeRequest& request) noexcept {
    if (conflicts_with(request, state.active)) {
        return false;
    }

    // A later overlapping reader cannot bypass an already queued writer.
    // Non-overlapping ranges remain independently progressable.
    return std::none_of(state.waiting.begin(), state.waiting.end(),
                        [&](const auto& waiter) {
                            return conflicts(request, *waiter);
                        });
}

[[nodiscard]] std::vector<std::shared_ptr<RangeRequest>> collect_grants(
    RangeState& state) {
    std::vector<std::shared_ptr<RangeRequest>> granted;
    std::vector<std::shared_ptr<RangeRequest>> earlier_blocked;

    for (auto iterator = state.waiting.begin(); iterator != state.waiting.end();) {
        const auto& request = *iterator;
        const bool blocked_by_active = conflicts_with(*request, state.active);
        const bool blocked_by_phase =
            conflicts_with(*request, earlier_blocked);
        if (blocked_by_active || blocked_by_phase) {
            earlier_blocked.push_back(request);
            ++iterator;
            continue;
        }

        request->granted = true;
        state.active.push_back(request);
        granted.push_back(request);
        iterator = state.waiting.erase(iterator);
    }
    return granted;
}

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

    auto target = detail::current_resume_target();
    if (target.runtime == nullptr || target.worker == detail::no_worker) {
        error_ = make_error_code(Errc::not_in_runtime);
        return false;
    }

    request_->coroutine = coroutine;
    request_->resume_target = target;

    std::lock_guard lock(state_->mutex);
    if (detail::can_grant_immediately(*state_, *request_)) {
        request_->granted = true;
        request_->pin_applied = false;
        state_->active.push_back(request_);
        return false;
    }

    state_->waiting.push_back(request_);
    return true;
}

Result<RangePermit> RangeAcquireAwaiter::await_resume() noexcept {
    if (error_) {
        return Result<RangePermit>::failure(error_);
    }
    if (!request_ || !request_->granted) {
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
        // A synchronous destructor cannot safely wake coroutine waiters. The
        // lock contract therefore requires explicit `co_await release()`.
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
        // release() has already transferred ownership out of RangePermit. If
        // the returned awaiter is discarded, no synchronous path can safely
        // wake range waiters or repair the Runtime pin registry.
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

    std::vector<std::shared_ptr<detail::RangeRequest>> granted;
    {
        std::lock_guard lock(state_->mutex);
        auto active = std::find(state_->active.begin(), state_->active.end(),
                                request_);
        if (active == state_->active.end()) {
            error_ = make_error_code(Errc::invalid_handle);
            return releaser;
        }
        state_->active.erase(active);
        granted = detail::collect_grants(*state_);
    }

    // A permit is movable and may cross a nested Task boundary. The current
    // releasing frame is then the registry owner after Task's symmetric pin
    // transfer; the original acquisition frame may already be destroyed.
    detail::unpin_current(releaser);
    auto releaser_target = detail::current_resume_target();

    if (granted.empty()) {
        // Returning the already-suspended releaser is an inline symmetric
        // continuation and does not touch either ready queue.
        return releaser;
    }

    std::shared_ptr<detail::RangeRequest> direct;
    for (auto& request : granted) {
        auto target = request->resume_target;
        if (target.pin_depth == 0 && !target.owner) {
            // The newly acquired range becomes this coroutine's first pin, so
            // ownership can be handed to the releasing worker.
            target.runtime = releaser_target.runtime;
            target.worker = releaser_target.worker;
        }
        ++target.pin_depth;
        request->resume_target = target;
        request->pin_applied = true;
        detail::register_pin(request->coroutine, target);

        if (!direct && target.runtime == releaser_target.runtime &&
            target.worker == releaser_target.worker) {
            direct = request;
            continue;
        }
        detail::schedule_resume(target, request->coroutine, true);
    }

    if (!direct) {
        // All granted waiters were already pinned to other workers. They were
        // queued there, while the unpinned releaser can continue inline.
        return releaser;
    }

    detail::schedule_resume(releaser_target, releaser,
                            releaser_target.pin_depth != 0);
    detail::prepare_symmetric_resume(direct->resume_target);
    return direct->coroutine;
}

Result<void> RangePermit::ReleaseAwaiter::await_resume() const noexcept {
    return error_ ? Result<void>::failure(error_) : Result<void>::success();
}

RangeArbiter::RangeArbiter()
    : state_(std::make_shared<detail::RangeState>()) {}

RangeAcquireAwaiter RangeArbiter::acquire(std::uint64_t offset,
                                          std::uint64_t length,
                                          RangeMode mode) noexcept {
    return RangeAcquireAwaiter(state_, offset, length, mode);
}

std::size_t RangeArbiter::active_count() const noexcept {
    if (!state_) {
        return 0;
    }
    std::lock_guard lock(state_->mutex);
    return state_->active.size();
}

std::size_t RangeArbiter::waiting_count() const noexcept {
    if (!state_) {
        return 0;
    }
    std::lock_guard lock(state_->mutex);
    return state_->waiting.size();
}

} // namespace orchfs::async
