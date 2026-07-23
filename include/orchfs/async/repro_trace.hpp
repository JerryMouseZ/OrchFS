#ifndef ORCHFS_ASYNC_REPRO_TRACE_HPP
#define ORCHFS_ASYNC_REPRO_TRACE_HPP

#include "orchfs/repro_trace.h"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <system_error>

namespace orchfs::async::repro_trace {

// A signed descriptor/reply status carries the error as its magnitude; trace
// records store the positive errno.
inline int error_from_status(std::int32_t status) noexcept {
  const std::int64_t magnitude = status < 0
      ? -static_cast<std::int64_t>(status)
      : static_cast<std::int64_t>(status);
  return magnitude > std::numeric_limits<int>::max()
      ? static_cast<int>(std::errc::protocol_error)
      : static_cast<int>(magnitude);
}

class Span final {
 public:
  explicit Span(orchfs_repro_trace_stage stage, std::uint64_t request_id = 0,
                std::uint64_t requested_bytes = 0,
                std::uint32_t child_io_count = 1) noexcept
      : stage_(stage), request_id_(request_id), bytes_(requested_bytes),
        child_io_count_(child_io_count),
        started_ns_(orchfs_repro_trace_begin()) {}

  Span(const Span&) = delete;
  Span& operator=(const Span&) = delete;

  ~Span() { finish(bytes_, 0); }

  void finish(std::uint64_t bytes, int error_number = 0) noexcept {
    if (started_ns_ == 0) {
      return;
    }
    orchfs_repro_trace_end(stage_, request_id_, started_ns_, bytes,
                           child_io_count_, error_number);
    started_ns_ = 0;
  }

  void finish(std::uint64_t bytes, std::error_code error) noexcept {
    finish(bytes, error ? error.value() : 0);
  }

  void add_bytes(std::uint64_t bytes) noexcept {
    constexpr auto maximum = std::numeric_limits<std::uint64_t>::max();
    if (bytes_ > maximum - bytes) {
      bytes_ = maximum;
      return;
    }
    bytes_ += bytes;
  }

 private:
  orchfs_repro_trace_stage stage_;
  std::uint64_t request_id_;
  std::uint64_t bytes_;
  std::uint32_t child_io_count_;
  std::uint64_t started_ns_;
};

}  // namespace orchfs::async::repro_trace

#endif
