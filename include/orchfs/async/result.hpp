#pragma once

#include <concepts>
#include <exception>
#include <functional>
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
class Result;

template <>
class Result<void>;

namespace detail {

template <typename T>
[[nodiscard, gnu::always_inline]] Result<T>
make_result_failure(std::error_code error) noexcept;

template <typename T, typename... Arguments>
[[nodiscard, gnu::always_inline]] Result<T>
make_result_success(Arguments&&... arguments);

template <typename>
struct IsResult : std::false_type {};

template <typename T>
struct IsResult<Result<T>> : std::true_type {};

template <typename T>
inline constexpr bool is_result_v =
    IsResult<std::remove_cvref_t<T>>::value;

} // namespace detail

template <typename T>
class [[nodiscard]] Result {
    static_assert(!std::is_reference_v<T>);

public:
    using value_type = T;

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

    template <typename Function>
        requires detail::is_result_v<std::invoke_result_t<Function, T&>>
    [[nodiscard, gnu::always_inline]] auto and_then(Function&& function) & {
        return and_then_impl(*this, std::forward<Function>(function));
    }

    template <typename Function>
        requires detail::is_result_v<
            std::invoke_result_t<Function, const T&>>
    [[nodiscard, gnu::always_inline]] auto and_then(
        Function&& function) const& {
        return and_then_impl(*this, std::forward<Function>(function));
    }

    template <typename Function>
        requires detail::is_result_v<std::invoke_result_t<Function, T&&>>
    [[nodiscard, gnu::always_inline]] auto and_then(Function&& function) && {
        return and_then_impl(std::move(*this),
                             std::forward<Function>(function));
    }

    template <typename Function>
        requires std::invocable<Function, T&>
    [[nodiscard, gnu::always_inline]] auto transform(Function&& function) & {
        return transform_impl(*this, std::forward<Function>(function));
    }

    template <typename Function>
        requires std::invocable<Function, const T&>
    [[nodiscard, gnu::always_inline]] auto transform(
        Function&& function) const& {
        return transform_impl(*this, std::forward<Function>(function));
    }

    template <typename Function>
        requires std::invocable<Function, T&&>
    [[nodiscard, gnu::always_inline]] auto transform(Function&& function) && {
        return transform_impl(std::move(*this),
                              std::forward<Function>(function));
    }

private:
    struct ValueTag {};
    struct ErrorTag {};

    template <typename U>
    explicit Result(ValueTag, U&& value)
        : value_(std::in_place, std::forward<U>(value)) {}

    explicit Result(ErrorTag, std::error_code error) noexcept
        : error_(error ? error : std::make_error_code(std::errc::io_error)) {}

    template <typename Self, typename Function>
    [[nodiscard, gnu::always_inline]] static auto and_then_impl(
        Self&& self, Function&& function) {
        using Argument = decltype(*std::forward<Self>(self).value_);
        using Next = std::remove_cvref_t<
            std::invoke_result_t<Function, Argument>>;
        if (!self.value_) {
            return Next::failure(self.error_);
        }
        return std::invoke(std::forward<Function>(function),
                           *std::forward<Self>(self).value_);
    }

    template <typename Self, typename Function>
    [[nodiscard, gnu::always_inline]] static auto transform_impl(
        Self&& self, Function&& function) {
        using Argument = decltype(*std::forward<Self>(self).value_);
        using Transformed = std::invoke_result_t<Function, Argument>;
        using Value = std::conditional_t<std::is_void_v<Transformed>, void,
                                         std::remove_cvref_t<Transformed>>;
        if (!self.value_) {
            return detail::make_result_failure<Value>(self.error_);
        }
        if constexpr (std::is_void_v<Transformed>) {
            std::invoke(std::forward<Function>(function),
                        *std::forward<Self>(self).value_);
            return detail::make_result_success<Value>();
        } else {
            return detail::make_result_success<Value>(
                std::invoke(std::forward<Function>(function),
                            *std::forward<Self>(self).value_));
        }
    }

    std::optional<T> value_;
    std::error_code error_;
};

template <>
class [[nodiscard]] Result<void> {
public:
    using value_type = void;

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

    template <typename Function>
        requires detail::is_result_v<std::invoke_result_t<Function>>
    [[nodiscard, gnu::always_inline]] auto and_then(
        Function&& function) const {
        using Next = std::remove_cvref_t<std::invoke_result_t<Function>>;
        if (error_) {
            return Next::failure(error_);
        }
        return std::invoke(std::forward<Function>(function));
    }

    template <typename Function>
        requires std::invocable<Function>
    [[nodiscard, gnu::always_inline]] auto transform(
        Function&& function) const {
        using Transformed = std::invoke_result_t<Function>;
        if (error_) {
            if constexpr (std::is_void_v<Transformed>) {
                return Result<void>::failure(error_);
            } else {
                return Result<std::remove_cvref_t<Transformed>>::failure(
                    error_);
            }
        }
        if constexpr (std::is_void_v<Transformed>) {
            std::invoke(std::forward<Function>(function));
            return Result<void>::success();
        } else {
            return Result<std::remove_cvref_t<Transformed>>::success(
                std::invoke(std::forward<Function>(function)));
        }
    }

private:
    explicit Result(std::error_code error) noexcept
        : error_(error) {}

    std::error_code error_;
};

namespace detail {

template <typename T>
[[nodiscard, gnu::always_inline]] inline Result<T>
make_result_failure(std::error_code error) noexcept {
    return Result<T>::failure(error);
}

template <typename T, typename... Arguments>
[[nodiscard, gnu::always_inline]] inline Result<T>
make_result_success(Arguments&&... arguments) {
    if constexpr (std::is_void_v<T>) {
        static_assert(sizeof...(Arguments) == 0);
        return Result<void>::success();
    } else {
        return Result<T>::success(
            std::forward<Arguments>(arguments)...);
    }
}

class ResultFailure final {
public:
    explicit ResultFailure(std::error_code error) noexcept : error_(error) {}

    template <typename T>
    [[nodiscard, gnu::always_inline]] operator Result<T>() const noexcept {
        return Result<T>::failure(error_);
    }

private:
    std::error_code error_;
};

[[nodiscard, gnu::always_inline]] inline ResultFailure
propagate_failure(std::error_code error) noexcept {
    return ResultFailure(error);
}

} // namespace detail

} // namespace orchfs::async

// These macros are block-scope coroutine statements. ORCHFS_TRY keeps the
// successful object in its Result storage and binds an rvalue reference to it,
// avoiding an extra move on hot paths. ORCHFS_TRYV validates and discards any
// successful value.
#define ORCHFS_DETAIL_CONCAT_INNER(left, right) left##right
#define ORCHFS_DETAIL_CONCAT(left, right) \
    ORCHFS_DETAIL_CONCAT_INNER(left, right)

#define ORCHFS_DETAIL_TRY_BIND(name, expression, id)                         \
    auto ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id) = (expression);       \
    if (!ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id)) {                    \
        co_return ::orchfs::async::detail::propagate_failure(                \
            ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id).error());          \
    }                                                                        \
    auto&& name = std::move(ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id))   \
                      .value()

#define ORCHFS_TRY(name, expression) \
    ORCHFS_DETAIL_TRY_BIND(name, expression, __COUNTER__)

#define ORCHFS_DETAIL_TRY_VOID(expression, id)                               \
    do {                                                                     \
        auto ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id) = (expression);   \
        if (!ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id)) {                \
            co_return ::orchfs::async::detail::propagate_failure(            \
                ORCHFS_DETAIL_CONCAT(_orchfs_try_result_, id).error());      \
        }                                                                    \
    } while (false)

#define ORCHFS_TRYV(expression) \
    ORCHFS_DETAIL_TRY_VOID(expression, __COUNTER__)

template <>
struct std::is_error_code_enum<orchfs::async::Errc> : true_type {};
