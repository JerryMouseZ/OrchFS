#ifndef ORCHFS_REPRO_TRACE_H
#define ORCHFS_REPRO_TRACE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum orchfs_repro_trace_stage {
  ORCHFS_TRACE_CLIENT_ROUND_TRIP = 1,
  ORCHFS_TRACE_SERVER_DISPATCH = 2,
  ORCHFS_TRACE_CORE_READ = 3,
  ORCHFS_TRACE_CORE_WRITE = 4,
  ORCHFS_TRACE_CORE_SYNC = 5,
  ORCHFS_TRACE_DEVICE_READ = 6,
  ORCHFS_TRACE_DEVICE_WRITE = 7,
  ORCHFS_TRACE_DEVICE_FLUSH = 8,
  ORCHFS_TRACE_NVM_READ = 9,
  ORCHFS_TRACE_NVM_WRITE = 10,
  ORCHFS_TRACE_SPDK_READ = 11,
  ORCHFS_TRACE_SPDK_WRITE = 12,
  ORCHFS_TRACE_SPDK_FLUSH = 13,
  ORCHFS_TRACE_JOURNAL_COMMIT = 14,
  ORCHFS_TRACE_CORE_PREPARE_WRITE = 15,
  ORCHFS_TRACE_CORE_ENSURE_STRATA = 16,
  ORCHFS_TRACE_CORE_PUBLISH_EXTENT = 17,
  ORCHFS_TRACE_STRATA_ALLOCATE = 18,
  ORCHFS_TRACE_STRATA_INSERT = 19,
};

#ifdef ORCHFS_REPRO_TRACE_ENABLED

uint64_t orchfs_repro_trace_begin(void);
void orchfs_repro_trace_end(enum orchfs_repro_trace_stage stage,
                            uint64_t request_id, uint64_t started_ns,
                            uint64_t bytes, uint32_t child_io_count,
                            int error_number);
void orchfs_repro_trace_flush(void);

#else

static inline uint64_t orchfs_repro_trace_begin(void) { return 0; }
static inline void orchfs_repro_trace_end(
    enum orchfs_repro_trace_stage stage, uint64_t request_id,
    uint64_t started_ns, uint64_t bytes, uint32_t child_io_count,
    int error_number) {
  (void)stage;
  (void)request_id;
  (void)started_ns;
  (void)bytes;
  (void)child_io_count;
  (void)error_number;
}
static inline void orchfs_repro_trace_flush(void) {}

#endif

#ifdef __cplusplus
}
#endif

#endif
