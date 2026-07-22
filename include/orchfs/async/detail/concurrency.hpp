#pragma once

#include <atomic>
#include <immintrin.h>

namespace orchfs::async::detail {

template <bool kPause = true>
class AtomicFlagGuard final {
public:
    [[gnu::always_inline]] explicit AtomicFlagGuard(
        std::atomic_flag& flag) noexcept
        : flag_(flag) {
        while (flag_.test_and_set(std::memory_order_acquire)) {
            if constexpr (kPause) {
                _mm_pause();
            }
        }
    }

    [[gnu::always_inline]] ~AtomicFlagGuard() {
        flag_.clear(std::memory_order_release);
    }

    AtomicFlagGuard(const AtomicFlagGuard&) = delete;
    AtomicFlagGuard& operator=(const AtomicFlagGuard&) = delete;

private:
    std::atomic_flag& flag_;
};

} // namespace orchfs::async::detail
