#pragma once

#include "orchfs/async/range_arbiter.hpp"

#include <utility>

namespace orchfs::async::detail {

template <typename T>
[[gnu::always_inline]] inline void fold_range_lock_release(
    Result<T>& outcome, const Result<void>& released) {
    if (!released && outcome) {
        outcome = Result<T>::failure(released.error());
    }
}

} // namespace orchfs::async::detail

// A block-scope coroutine statement that keeps acquire, operation, and release
// in the caller's frame. The operation and caller must both use Result<T>.
#define ORCHFS_DETAIL_WITH_RANGE_LOCK(                                      \
    name, gate, offset, length, mode, id, ...)                              \
    auto ORCHFS_DETAIL_CONCAT(_orchfs_range_acquired_, id) =                \
        co_await (gate).acquire((offset), (length), (mode));                 \
    if (!ORCHFS_DETAIL_CONCAT(_orchfs_range_acquired_, id)) {               \
        co_return ::orchfs::async::detail::propagate_failure(               \
            ORCHFS_DETAIL_CONCAT(_orchfs_range_acquired_, id).error());     \
    }                                                                        \
    auto ORCHFS_DETAIL_CONCAT(_orchfs_range_permit_, id) =                  \
        std::move(ORCHFS_DETAIL_CONCAT(_orchfs_range_acquired_, id))        \
            .value();                                                        \
    auto name = co_await (__VA_ARGS__);                                     \
    static_assert(::orchfs::async::detail::is_result_v<decltype(name)>);    \
    auto ORCHFS_DETAIL_CONCAT(_orchfs_range_released_, id) =                \
        co_await ORCHFS_DETAIL_CONCAT(_orchfs_range_permit_, id).release(); \
    ::orchfs::async::detail::fold_range_lock_release(                       \
        name, ORCHFS_DETAIL_CONCAT(_orchfs_range_released_, id))

#define ORCHFS_WITH_RANGE_LOCK(name, gate, offset, length, mode, ...)       \
    ORCHFS_DETAIL_WITH_RANGE_LOCK(                                          \
        name, gate, offset, length, mode, __COUNTER__, __VA_ARGS__)
