#pragma once

#include <cstdint>

namespace orchfs::async::detail {

template <typename Output, typename Input>
[[nodiscard, gnu::always_inline]] inline Output copy_file_stat_fields(
    const Input& input) noexcept {
  Output output{};
#define ORCHFS_ASYNC_FILE_STAT_FIELD(type, name, posix_name) \
  output.name = input.name;
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILE_STAT_FIELD
  return output;
}

template <typename Output, typename Input>
[[nodiscard, gnu::always_inline]] inline Output copy_filesystem_stat_fields(
    const Input& input) noexcept {
  Output output{};
#define ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD(type, name, posix_name) \
  output.name = input.name;
#include "orchfs/async/detail/stat_fields.inc"
#undef ORCHFS_ASYNC_FILESYSTEM_STAT_FIELD
  return output;
}

}  // namespace orchfs::async::detail
