#pragma once

#include "orchfs/async/result.hpp"

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <immintrin.h>
#include <system_error>

namespace orchfs::async::detail {

template <typename Node>
struct MemberNextLink {
    [[nodiscard, gnu::always_inline]] static Node* next(
        const Node& node) noexcept {
        return node.next;
    }

    [[gnu::always_inline]] static void set_next(Node& node,
                                                Node* next) noexcept {
        node.next = next;
    }
};

template <typename Node, typename Link = MemberNextLink<Node>>
class MpscInbox final {
public:
    MpscInbox() = default;
    MpscInbox(const MpscInbox&) = delete;
    MpscInbox& operator=(const MpscInbox&) = delete;

    [[nodiscard, gnu::always_inline]] bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) == nullptr;
    }

    [[gnu::always_inline]] void push(Node& node) noexcept {
        Node* head = head_.load(std::memory_order_relaxed);
        do {
            Link::set_next(node, head);
        } while (!head_.compare_exchange_weak(
            head, &node, std::memory_order_release,
            std::memory_order_relaxed));
    }

    [[nodiscard, gnu::always_inline]] Node* take_all() noexcept {
        return head_.exchange(nullptr, std::memory_order_acquire);
    }

    [[nodiscard, gnu::always_inline]] Node* drain() noexcept {
        Node* stack = take_all();
        Node* fifo = nullptr;
        while (stack != nullptr) {
            Node* next = Link::next(*stack);
            Link::set_next(*stack, fifo);
            fifo = stack;
            stack = next;
        }
        return fifo;
    }

    [[gnu::always_inline]] void clear() noexcept {
        head_.store(nullptr, std::memory_order_release);
    }

private:
    std::atomic<Node*> head_{nullptr};
};

template <bool kNormalize = true>
[[nodiscard, gnu::always_inline]] inline std::error_code
errno_error(int error) noexcept {
    if constexpr (kNormalize) {
        error = error > 0 ? error : EIO;
    }
    return {error, std::generic_category()};
}

template <typename T>
[[nodiscard, gnu::always_inline]] inline Result<T>
errno_failure(int error) noexcept {
    return Result<T>::failure(errno_error(error));
}

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
