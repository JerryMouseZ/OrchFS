#pragma once

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <new>
#include <sched.h>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace orchfs::async::detail {

// Parse an ordered, comma-separated Runtime CPU list.  Keep this stricter
// than taskset syntax: Runtime assigns one worker to every entry, so ranges,
// whitespace, empty entries, and duplicates are configuration errors.
[[nodiscard]] inline std::error_code parse_cpu_list(
    std::string_view input, std::vector<unsigned>& cpus) noexcept {
    if (input.empty()) {
        return std::make_error_code(std::errc::invalid_argument);
    }

    try {
        std::vector<unsigned> parsed;
        std::size_t cursor = 0;
        while (cursor < input.size()) {
            const std::size_t comma = input.find(',', cursor);
            const std::string_view token = input.substr(
                cursor, comma == std::string_view::npos
                            ? input.size() - cursor
                            : comma - cursor);
            if (token.empty()) {
                return std::make_error_code(std::errc::invalid_argument);
            }

            unsigned cpu{};
            const auto [end, error] = std::from_chars(
                token.data(), token.data() + token.size(), cpu);
            if (error != std::errc{} || end != token.data() + token.size() ||
                cpu >= CPU_SETSIZE ||
                std::find(parsed.begin(), parsed.end(), cpu) != parsed.end()) {
                return std::make_error_code(std::errc::invalid_argument);
            }
            parsed.push_back(cpu);

            if (comma == std::string_view::npos) {
                break;
            }
            cursor = comma + 1;
        }

        cpus = std::move(parsed);
        return {};
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
}

} // namespace orchfs::async::detail
