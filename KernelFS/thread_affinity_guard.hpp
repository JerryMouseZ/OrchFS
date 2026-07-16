#ifndef ORCHFS_KERNELFS_THREAD_AFFINITY_GUARD_HPP
#define ORCHFS_KERNELFS_THREAD_AFFINITY_GUARD_HPP

#include <pthread.h>
#include <sched.h>

#include <system_error>

namespace orchfs::nvme::detail {

// Captures the current thread's complete allowed CPU set. restore() reports
// failures to the caller; the destructor makes a final best-effort attempt so
// exceptional exits cannot silently leave the thread pinned.
class ThreadAffinityGuard final {
  public:
    explicit ThreadAffinityGuard(std::error_code &error) noexcept {
        const int result = ::pthread_getaffinity_np(
            ::pthread_self(), sizeof(affinity_), &affinity_);
        if (result != 0) {
            error = {result, std::generic_category()};
            return;
        }
        captured_ = true;
        error.clear();
    }

    ~ThreadAffinityGuard() {
        if (captured_) {
            (void)::pthread_setaffinity_np(
                ::pthread_self(), sizeof(affinity_), &affinity_);
        }
    }

    ThreadAffinityGuard(const ThreadAffinityGuard &) = delete;
    ThreadAffinityGuard &operator=(const ThreadAffinityGuard &) = delete;

    [[nodiscard]] std::error_code restore() noexcept {
        if (!captured_) {
            return std::make_error_code(std::errc::state_not_recoverable);
        }
        const int result = ::pthread_setaffinity_np(
            ::pthread_self(), sizeof(affinity_), &affinity_);
        if (result != 0) {
            return {result, std::generic_category()};
        }
        captured_ = false;
        return {};
    }

  private:
    cpu_set_t affinity_{};
    bool captured_{};
};

} // namespace orchfs::nvme::detail

#endif // ORCHFS_KERNELFS_THREAD_AFFINITY_GUARD_HPP
