#pragma once

#include <atomic>
#include <cstdint>
#include <immintrin.h>

namespace orchfs::async::detail {

// Some fixed-size tables historically use only the first mixing round. The
// template keeps those indices bit-for-bit stable while sharing the sequence.
template <bool kComplete = true>
[[nodiscard, gnu::always_inline]] constexpr std::uint64_t
fmix64(std::uint64_t value) noexcept {
    value ^= value >> 33U;
    value *= 0xff51afd7ed558ccdULL;
    value ^= value >> 33U;
    if constexpr (kComplete) {
        value *= 0xc4ceb9fe1a85ec53ULL;
        value ^= value >> 33U;
    }
    return value;
}

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
