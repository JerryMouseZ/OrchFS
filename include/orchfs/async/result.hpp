#pragma once

#include <concepts>
#include <exception>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <utility>

namespace orchfs::async {

enum class Errc {
    runtime_stopping = 1,
    runtime_already_exists,
    invalid_task,
    invalid_handle,
    already_consumed,
    join_from_worker,
    not_in_runtime,
    wrong_runtime,
    invalid_worker,
    pinned_to_worker,
    invalid_range,
};

class ErrorCategory final : public std::error_category {
public:
    [[nodiscard]] const char* name() const noexcept override {
        return "orchfs.async";
    }

    [[nodiscard]] std::string message(int value) const override {
        switch (static_cast<Errc>(value)) {
        case Errc::runtime_stopping:
            return "runtime is stopping";
        case Errc::runtime_already_exists:
            return "a runtime already exists in this process";
        case Errc::invalid_task:
            return "invalid task";
        case Errc::invalid_handle:
            return "invalid join handle";
        case Errc::already_consumed:
            return "result already consumed";
        case Errc::join_from_worker:
            return "blocking join from a runtime worker";
        case Errc::not_in_runtime:
            return "operation requires a runtime worker";
        case Errc::wrong_runtime:
            return "operation belongs to another runtime";
        case Errc::invalid_worker:
            return "invalid runtime worker";
        case Errc::pinned_to_worker:
            return "coroutine is pinned to another worker";
        case Errc::invalid_range:
            return "invalid byte range";
        }
        return "unknown OrchFS async error";
    }
};

[[nodiscard]] inline const std::error_category& error_category() noexcept {
    static ErrorCategory category;
    return category;
}

[[nodiscard]] inline std::error_code make_error_code(Errc error) noexcept {
    return {static_cast<int>(error), error_category()};
}

template <typename T>
class [[nodiscard]] Result {
    static_assert(!std::is_reference_v<T>);

public:
    Result(const Result&) = default;
    Result(Result&&) noexcept(std::is_nothrow_move_constructible_v<T>) = default;
    Result& operator=(const Result&) = default;
    Result& operator=(Result&&) noexcept(std::is_nothrow_move_assignable_v<T>) = default;

    template <typename U = T>
        requires std::constructible_from<T, U&&>
    [[nodiscard]] static Result success(U&& value) {
        return Result(ValueTag{}, std::forward<U>(value));
    }

    [[nodiscard]] static Result failure(std::error_code error) noexcept {
        return Result(ErrorTag{}, error);
    }

    [[nodiscard]] static Result failure(Errc error) noexcept {
        return failure(make_error_code(error));
    }

    [[nodiscard]] bool has_value() const noexcept {
        return value_.has_value();
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value();
    }

    [[nodiscard]] T& value() & noexcept {
        if (!value_) {
            std::terminate();
        }
        return *value_;
    }

    [[nodiscard]] const T& value() const& noexcept {
        if (!value_) {
            std::terminate();
        }
        return *value_;
    }

    [[nodiscard]] T&& value() && noexcept {
        if (!value_) {
            std::terminate();
        }
        return std::move(*value_);
    }

    [[nodiscard]] std::error_code error() const noexcept {
        return value_ ? std::error_code{} : error_;
    }

private:
    struct ValueTag {};
    struct ErrorTag {};

    template <typename U>
    explicit Result(ValueTag, U&& value)
        : value_(std::in_place, std::forward<U>(value)) {}

    explicit Result(ErrorTag, std::error_code error) noexcept
        : error_(error ? error : std::make_error_code(std::errc::io_error)) {}

    std::optional<T> value_;
    std::error_code error_;
};

template <>
class [[nodiscard]] Result<void> {
public:
    [[nodiscard]] static Result success() noexcept {
        return Result({});
    }

    [[nodiscard]] static Result failure(std::error_code error) noexcept {
        return Result(error ? error : std::make_error_code(std::errc::io_error));
    }

    [[nodiscard]] static Result failure(Errc error) noexcept {
        return failure(make_error_code(error));
    }

    [[nodiscard]] bool has_value() const noexcept {
        return !error_;
    }

    [[nodiscard]] explicit operator bool() const noexcept {
        return has_value();
    }

    [[nodiscard]] std::error_code error() const noexcept {
        return error_;
    }

private:
    explicit Result(std::error_code error) noexcept
        : error_(error) {}

    std::error_code error_;
};

namespace detail {

template <typename>
struct IsResult : std::false_type {};

template <typename T>
struct IsResult<Result<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_result_v = IsResult<std::remove_cv_t<T>>::value;

} // namespace detail

} // namespace orchfs::async

template <>
struct std::is_error_code_enum<orchfs::async::Errc> : true_type {};
