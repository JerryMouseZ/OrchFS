#pragma once

#include "orchfs/async/task.hpp"

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <system_error>

namespace orchfs::async {

enum class RangeMode {
    read,
    write,
};

namespace detail {
struct RangeState;
struct RangeRequest;
} // namespace detail

class RangePermit;

class RangeAcquireAwaiter {
public:
    RangeAcquireAwaiter(const RangeAcquireAwaiter&) = delete;
    RangeAcquireAwaiter& operator=(const RangeAcquireAwaiter&) = delete;
    RangeAcquireAwaiter(RangeAcquireAwaiter&&) noexcept = default;
    RangeAcquireAwaiter& operator=(RangeAcquireAwaiter&&) noexcept = default;
    ~RangeAcquireAwaiter() = default;

    [[nodiscard]] bool await_ready() const noexcept {
        return false;
    }

    [[nodiscard]] bool await_suspend(std::coroutine_handle<> coroutine) noexcept;
    [[nodiscard]] Result<RangePermit> await_resume() noexcept;

private:
    RangeAcquireAwaiter(std::shared_ptr<detail::RangeState> state,
                        std::uint64_t offset,
                        std::uint64_t length,
                        RangeMode mode) noexcept;

    std::shared_ptr<detail::RangeState> state_;
    std::shared_ptr<detail::RangeRequest> request_;
    std::error_code error_;

    friend class RangeArbiter;
};

class [[nodiscard]] RangePermit {
public:
    RangePermit() = default;
    RangePermit(const RangePermit&) = delete;
    RangePermit& operator=(const RangePermit&) = delete;
    RangePermit(RangePermit&& other) noexcept;
    RangePermit& operator=(RangePermit&& other) noexcept;
    ~RangePermit();

    [[nodiscard]] bool owns_lock() const noexcept {
        return owns_;
    }

    [[nodiscard]] std::uint64_t first_block() const noexcept;
    [[nodiscard]] std::uint64_t block_count() const noexcept;
    [[nodiscard]] RangeMode mode() const noexcept;

    class ReleaseAwaiter {
    public:
        ReleaseAwaiter(const ReleaseAwaiter&) = delete;
        ReleaseAwaiter& operator=(const ReleaseAwaiter&) = delete;
        ReleaseAwaiter(ReleaseAwaiter&& other) noexcept;
        ReleaseAwaiter& operator=(ReleaseAwaiter&&) noexcept = delete;
        ~ReleaseAwaiter();

        [[nodiscard]] bool await_ready() const noexcept {
            return false;
        }

        [[nodiscard]] std::coroutine_handle<> await_suspend(
            std::coroutine_handle<> releaser) noexcept;
        [[nodiscard]] Result<void> await_resume() const noexcept;

    private:
        ReleaseAwaiter(std::shared_ptr<detail::RangeState> state,
                       std::shared_ptr<detail::RangeRequest> request,
                       std::error_code error) noexcept
            : state_(std::move(state)), request_(std::move(request)),
              error_(error), armed_(state_ && request_ && !error_) {}

        std::shared_ptr<detail::RangeState> state_;
        std::shared_ptr<detail::RangeRequest> request_;
        std::error_code error_;
        bool armed_{false};

        friend class RangePermit;
    };

    [[nodiscard]] ReleaseAwaiter release() noexcept;

private:
    RangePermit(std::shared_ptr<detail::RangeState> state,
                std::shared_ptr<detail::RangeRequest> request) noexcept
        : state_(std::move(state)), request_(std::move(request)), owns_(true) {}

    void reset_without_release() noexcept;

    std::shared_ptr<detail::RangeState> state_;
    std::shared_ptr<detail::RangeRequest> request_;
    bool owns_{false};

    friend class RangeAcquireAwaiter;
};

class RangeArbiter final {
public:
    static constexpr std::uint64_t granularity = 32U * 1024U;

    RangeArbiter();
    RangeArbiter(const RangeArbiter&) = delete;
    RangeArbiter& operator=(const RangeArbiter&) = delete;
    RangeArbiter(RangeArbiter&&) noexcept = default;
    RangeArbiter& operator=(RangeArbiter&&) noexcept = default;
    ~RangeArbiter() = default;

    [[nodiscard]] RangeAcquireAwaiter acquire(std::uint64_t offset,
                                               std::uint64_t length,
                                               RangeMode mode) noexcept;

    [[nodiscard]] std::size_t active_count() const noexcept;
    [[nodiscard]] std::size_t waiting_count() const noexcept;

private:
    std::shared_ptr<detail::RangeState> state_;
};

} // namespace orchfs::async
