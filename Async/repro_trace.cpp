#include "orchfs/repro_trace.h"

#ifdef ORCHFS_REPRO_TRACE_ENABLED

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <new>
#include <string_view>
#include <time.h>
#include <unistd.h>

#include <sys/syscall.h>

namespace {

struct Record {
  std::uint64_t sequence{};
  std::uint64_t request_id{};
  std::uint64_t started_ns{};
  std::uint64_t ended_ns{};
  std::uint64_t bytes{};
  std::uint32_t stage{};
  std::uint32_t child_io_count{};
  std::int32_t error_number{};
  std::int32_t tid{};
};

struct ThreadBuffer {
  Record* records{};
  std::size_t capacity{};
  std::size_t size{};
  std::uint64_t event_counter{};
  std::int32_t tid{};
  ThreadBuffer* next{};
};

std::atomic<ThreadBuffer*> buffers;
std::atomic<std::uint64_t> next_sequence{1};
std::atomic<std::uint64_t> dropped_records{0};
std::atomic_flag flushed = ATOMIC_FLAG_INIT;

std::size_t parse_size(const char* name, std::size_t fallback,
                       std::size_t maximum) noexcept {
  const char* text = std::getenv(name);
  if (text == nullptr || *text == '\0' || *text == '-') {
    return fallback;
  }
  errno = 0;
  char* end = nullptr;
  const unsigned long long value = std::strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0' || value == 0 ||
      value > maximum) {
    return fallback;
  }
  return static_cast<std::size_t>(value);
}

std::size_t buffer_capacity() noexcept {
  static const std::size_t value = parse_size(
      "ORCHFS_REPRO_TRACE_RECORDS_PER_THREAD", 65536, 4U * 1024U * 1024U);
  return value;
}

std::size_t sample_every() noexcept {
  static const std::size_t value = parse_size(
      "ORCHFS_REPRO_TRACE_SAMPLE_EVERY", 1, 1024U * 1024U);
  return value;
}

const char* output_path() noexcept {
  const char* path = std::getenv("ORCHFS_REPRO_TRACE_FILE");
  return path != nullptr && *path != '\0' ? path : nullptr;
}

ThreadBuffer* create_buffer() noexcept {
  const std::size_t capacity = buffer_capacity();
  if (capacity > std::numeric_limits<std::size_t>::max() / sizeof(Record)) {
    return nullptr;
  }
  auto* buffer = new (std::nothrow) ThreadBuffer;
  if (buffer == nullptr) {
    return nullptr;
  }
  buffer->records = static_cast<Record*>(
      std::calloc(capacity, sizeof(Record)));
  if (buffer->records == nullptr) {
    delete buffer;
    return nullptr;
  }
  buffer->capacity = capacity;
  buffer->tid = static_cast<std::int32_t>(::syscall(SYS_gettid));
  ThreadBuffer* head = buffers.load(std::memory_order_relaxed);
  do {
    buffer->next = head;
  } while (!buffers.compare_exchange_weak(
      head, buffer, std::memory_order_release, std::memory_order_relaxed));
  return buffer;
}

ThreadBuffer* local_buffer() noexcept {
  thread_local ThreadBuffer* value = create_buffer();
  return value;
}

std::uint64_t monotonic_raw_ns() noexcept {
  struct timespec value {};
  if (::clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0) {
    return 0;
  }
  return static_cast<std::uint64_t>(value.tv_sec) * 1000000000ULL +
         static_cast<std::uint64_t>(value.tv_nsec);
}

const char* stage_name(std::uint32_t stage) noexcept {
  switch (static_cast<orchfs_repro_trace_stage>(stage)) {
    case ORCHFS_TRACE_CLIENT_ROUND_TRIP:
      return "client_round_trip";
    case ORCHFS_TRACE_SERVER_DISPATCH:
      return "server_dispatch";
    case ORCHFS_TRACE_CORE_READ:
      return "core_read";
    case ORCHFS_TRACE_CORE_WRITE:
      return "core_write";
    case ORCHFS_TRACE_CORE_SYNC:
      return "core_sync";
    case ORCHFS_TRACE_DEVICE_READ:
      return "device_read";
    case ORCHFS_TRACE_DEVICE_WRITE:
      return "device_write";
    case ORCHFS_TRACE_DEVICE_FLUSH:
      return "device_flush";
    case ORCHFS_TRACE_NVM_READ:
      return "nvm_read";
    case ORCHFS_TRACE_NVM_WRITE:
      return "nvm_write";
    case ORCHFS_TRACE_SPDK_READ:
      return "spdk_read";
    case ORCHFS_TRACE_SPDK_WRITE:
      return "spdk_write";
    case ORCHFS_TRACE_SPDK_FLUSH:
      return "spdk_flush";
  }
  return "unknown";
}

}  // namespace

extern "C" std::uint64_t orchfs_repro_trace_begin(void) {
  if (output_path() == nullptr) {
    return 0;
  }
  ThreadBuffer* buffer = local_buffer();
  if (buffer == nullptr) {
    dropped_records.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }
  ++buffer->event_counter;
  if ((buffer->event_counter - 1) % sample_every() != 0) {
    return 0;
  }
  return monotonic_raw_ns();
}

extern "C" void orchfs_repro_trace_end(
    enum orchfs_repro_trace_stage stage, std::uint64_t request_id,
    std::uint64_t started_ns, std::uint64_t bytes,
    std::uint32_t child_io_count, int error_number) {
  if (started_ns == 0) {
    return;
  }
  ThreadBuffer* buffer = local_buffer();
  if (buffer == nullptr || buffer->size >= buffer->capacity) {
    dropped_records.fetch_add(1, std::memory_order_relaxed);
    return;
  }
  Record& record = buffer->records[buffer->size++];
  record.sequence = next_sequence.fetch_add(1, std::memory_order_relaxed);
  record.request_id = request_id;
  record.started_ns = started_ns;
  record.ended_ns = monotonic_raw_ns();
  record.bytes = bytes;
  record.stage = static_cast<std::uint32_t>(stage);
  record.child_io_count = child_io_count;
  record.error_number = error_number;
  record.tid = buffer->tid;
}

extern "C" void orchfs_repro_trace_flush(void) {
  const char* path = output_path();
  if (path == nullptr || flushed.test_and_set(std::memory_order_acq_rel)) {
    return;
  }
  FILE* output = std::fopen(path, "w");
  if (output == nullptr) {
    std::fprintf(stderr, "open OrchFS reproduction trace %s: %s\n", path,
                 std::strerror(errno));
    return;
  }
  std::fprintf(output,
               "sequence,pid,tid,request_id,stage,started_ns,ended_ns,"
               "duration_ns,bytes,child_io_count,error_number,sample_every\n");
  const auto pid = static_cast<long>(::getpid());
  for (ThreadBuffer* buffer = buffers.load(std::memory_order_acquire);
       buffer != nullptr; buffer = buffer->next) {
    for (std::size_t index = 0; index < buffer->size; ++index) {
      const Record& record = buffer->records[index];
      const std::uint64_t duration = record.ended_ns >= record.started_ns
                                         ? record.ended_ns - record.started_ns
                                         : 0;
      std::fprintf(output,
                   "%llu,%ld,%d,%llu,%s,%llu,%llu,%llu,%llu,%u,%d,%zu\n",
                   static_cast<unsigned long long>(record.sequence), pid,
                   record.tid,
                   static_cast<unsigned long long>(record.request_id),
                   stage_name(record.stage),
                   static_cast<unsigned long long>(record.started_ns),
                   static_cast<unsigned long long>(record.ended_ns),
                   static_cast<unsigned long long>(duration),
                   static_cast<unsigned long long>(record.bytes),
                   record.child_io_count, record.error_number, sample_every());
    }
  }
  std::fprintf(output, "# dropped_records=%llu\n",
               static_cast<unsigned long long>(
                   dropped_records.load(std::memory_order_relaxed)));
  if (std::fclose(output) != 0) {
    std::fprintf(stderr, "close OrchFS reproduction trace %s: %s\n", path,
                 std::strerror(errno));
  }
}

#else

// The header compiles every call out when tracing is disabled.  Keep one
// translation-unit symbol so the CMake target is never an empty archive.
extern "C" int orchfs_repro_trace_disabled_translation_unit;
int orchfs_repro_trace_disabled_translation_unit;

#endif
