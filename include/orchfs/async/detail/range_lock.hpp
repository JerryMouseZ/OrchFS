#pragma once

#include "orchfs/async/range_arbiter.hpp"

#include <functional>
#include <type_traits>
#include <utility>

namespace orchfs::async::detail {

template <typename>
struct TaskValue;

template <typename T>
struct TaskValue<Task<T>> {
    using type = T;
};

template <typename TaskType>
using task_value_t = typename TaskValue<std::remove_cvref_t<TaskType>>::type;

struct EmptyRangeLockState {};

class ReadyRangeLockSchedule final {
public:
    [[nodiscard, gnu::always_inline]] constexpr bool await_ready()
        const noexcept {
        return true;
    }

    constexpr void await_suspend(std::coroutine_handle<>) const noexcept {}

    [[nodiscard, gnu::always_inline]] Result<void> await_resume()
        const noexcept {
        return Result<void>::success();
    }
};

struct NoRangeLockSchedule {
    [[nodiscard, gnu::always_inline]] ReadyRangeLockSchedule
    operator()() const noexcept {
        return {};
    }
};

template <typename Outcome>
struct ResultRangeLockPolicy {
    [[nodiscard, gnu::always_inline]] static Outcome early_failure(
        EmptyRangeLockState&&, std::error_code error) noexcept {
        return Outcome::failure(error);
    }

    [[nodiscard, gnu::always_inline]] static bool succeeded(
        const Outcome& outcome) noexcept {
        return static_cast<bool>(outcome);
    }

    [[gnu::always_inline]] static void apply_failure(
        Outcome& outcome, std::error_code error) noexcept {
        outcome = Outcome::failure(error);
    }
};

template <typename Schedule, typename State, typename Operation,
          typename Finish, typename Policy>
inline auto with_range_lock(
    Schedule schedule, RangeArbiter& gate, std::uint64_t offset,
    std::uint64_t length, RangeMode mode, State state, Operation operation,
    Finish finish, Policy policy)
    -> Task<std::invoke_result_t<
        Finish&, State&&,
        task_value_t<std::invoke_result_t<Operation&, State&>>&&>> {
    using OperationResult =
        task_value_t<std::invoke_result_t<Operation&, State&>>;
    using Outcome = std::invoke_result_t<Finish&, State&&,
                                         OperationResult&&>;

    auto scheduled = co_await std::invoke(schedule);
    if (!scheduled) {
        co_return policy.early_failure(std::move(state), scheduled.error());
    }
    auto acquired = co_await gate.acquire(offset, length, mode);
    if (!acquired) {
        co_return policy.early_failure(std::move(state), acquired.error());
    }
    auto permit = std::move(acquired).value();
    auto operation_result = co_await std::invoke(operation, state);
    Outcome outcome = std::invoke(
        finish, std::move(state), std::move(operation_result));
    auto released = co_await permit.release();
    if (!released && policy.succeeded(outcome)) {
        policy.apply_failure(outcome, released.error());
    }
    co_return outcome;
}

template <typename Schedule, typename Operation>
[[gnu::always_inline]] inline auto with_range_lock(
    Schedule schedule, RangeArbiter& gate, std::uint64_t offset,
    std::uint64_t length, RangeMode mode, Operation operation)
    -> Task<task_value_t<std::invoke_result_t<Operation&>>> {
    using Outcome = task_value_t<std::invoke_result_t<Operation&>>;
    static_assert(is_result_v<Outcome>);

    return with_range_lock(
        std::move(schedule), gate, offset, length, mode,
        EmptyRangeLockState{},
        [operation = std::move(operation)](
            EmptyRangeLockState&) mutable {
            return std::invoke(operation);
        },
        [](EmptyRangeLockState&&, Outcome&& outcome) {
            return std::move(outcome);
        },
        ResultRangeLockPolicy<Outcome>{});
}

template <typename Operation>
[[gnu::always_inline]] inline auto with_range_lock(
    RangeArbiter& gate, std::uint64_t offset, std::uint64_t length,
    RangeMode mode, Operation operation)
    -> Task<task_value_t<std::invoke_result_t<Operation&>>> {
    return with_range_lock(NoRangeLockSchedule{}, gate, offset, length, mode,
                           std::move(operation));
}

} // namespace orchfs::async::detail
