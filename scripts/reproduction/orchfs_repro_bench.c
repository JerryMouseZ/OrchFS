#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <getopt.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define ORCHFS_FD_OFFSET 1048576

enum operation_kind { OP_READ, OP_WRITE };
enum access_kind { ACCESS_RANDOM, ACCESS_SEQUENTIAL, ACCESS_APPEND };
enum size_kind { SIZE_FIXED, SIZE_PM10, SIZE_UNIFORM };
enum sync_kind { SYNC_NONE, SYNC_EACH, SYNC_END };

struct options {
  const char *path_prefix;
  const char *series_path;
  const char *label;
  enum operation_kind operation;
  enum access_kind access;
  enum size_kind size_mode;
  enum sync_kind sync_mode;
  uint64_t seed;
  uint64_t fixed_size;
  uint64_t minimum_size;
  uint64_t maximum_size;
  uint64_t prepare_bytes;
  bool prepare_requested;
  uint64_t bytes_per_thread;
  double duration_seconds;
  uint64_t offset_alignment;
  uint64_t unaligned_to;
  unsigned threads;
  unsigned files;
  unsigned series_ms;
  size_t latency_sample_limit;
  bool cleanup;
  bool allow_host_fs;
};

struct series_state {
  atomic_uint_fast64_t *bytes;
  atomic_uint_fast64_t *operations;
  size_t slots;
  size_t report_slots;
  uint64_t started_ns;
  uint64_t interval_ns;
};

struct worker {
  pthread_t thread;
  pthread_barrier_t *barrier;
  atomic_int *shared_error;
  struct options *options;
  struct series_state *series;
  unsigned id;
  int fd;
  uint64_t rng;
  uint64_t bytes;
  uint64_t operations;
  uint64_t latency_sum_ns;
  uint64_t *latencies;
  size_t latency_capacity;
  size_t latency_count;
  uint64_t checksum;
};

static void usage(const char *program) {
  fprintf(stderr,
          "usage: %s --path-prefix /Or/name [options]\n"
          "  --operation read|write\n"
          "  --access random|sequential|append\n"
          "  --size-mode fixed|pm10|uniform --size N [--min-size N --max-size N]\n"
          "  --prepare-bytes N | --existing-bytes N\n"
          "  --bytes-per-thread N | --duration-sec N\n"
          "  --threads N --files N --fsync none|each|end\n"
          "  --offset-alignment N [--unaligned-to N]\n"
          "  --seed N --label text --series-ms N --series-file path\n"
          "  --latency-samples N --cleanup --allow-host-fs\n",
          program);
}

static uint64_t now_ns(void) {
  struct timespec value;
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0) {
    perror("clock_gettime");
    exit(2);
  }
  return (uint64_t)value.tv_sec * 1000000000ULL +
         (uint64_t)value.tv_nsec;
}

static bool parse_u64(const char *text, uint64_t *output) {
  if (text == NULL || *text == '\0' || *text == '-') {
    return false;
  }
  errno = 0;
  char *end = NULL;
  unsigned long long base = strtoull(text, &end, 10);
  if (errno != 0 || end == text) {
    return false;
  }
  uint64_t multiplier = 1;
  if (*end != '\0') {
    if (strcasecmp(end, "k") == 0 || strcasecmp(end, "kb") == 0) {
      multiplier = 1000ULL;
    } else if (strcasecmp(end, "kib") == 0) {
      multiplier = 1024ULL;
    } else if (strcasecmp(end, "m") == 0 ||
               strcasecmp(end, "mb") == 0) {
      multiplier = 1000ULL * 1000ULL;
    } else if (strcasecmp(end, "mib") == 0) {
      multiplier = 1024ULL * 1024ULL;
    } else if (strcasecmp(end, "g") == 0 ||
               strcasecmp(end, "gb") == 0) {
      multiplier = 1000ULL * 1000ULL * 1000ULL;
    } else if (strcasecmp(end, "gib") == 0) {
      multiplier = 1024ULL * 1024ULL * 1024ULL;
    } else if (strcasecmp(end, "t") == 0 ||
               strcasecmp(end, "tb") == 0) {
      multiplier = 1000ULL * 1000ULL * 1000ULL * 1000ULL;
    } else if (strcasecmp(end, "tib") == 0) {
      multiplier = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
    } else {
      return false;
    }
  }
  if (base > UINT64_MAX / multiplier) {
    return false;
  }
  *output = (uint64_t)base * multiplier;
  return true;
}

static bool parse_unsigned(const char *text, unsigned *output) {
  uint64_t value = 0;
  if (!parse_u64(text, &value) || value > UINT32_MAX) {
    return false;
  }
  *output = (unsigned)value;
  return true;
}

static bool parse_double(const char *text, double *output) {
  if (text == NULL || *text == '\0') {
    return false;
  }
  errno = 0;
  char *end = NULL;
  const double value = strtod(text, &end);
  if (errno != 0 || end == text || *end != '\0' || value < 0.0) {
    return false;
  }
  *output = value;
  return true;
}

static uint64_t splitmix64(uint64_t *state) {
  uint64_t value = (*state += 0x9e3779b97f4a7c15ULL);
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

static uint64_t random_bounded(uint64_t *state, uint64_t bound) {
  if (bound <= 1) {
    return 0;
  }
  const uint64_t threshold = (uint64_t)(-bound) % bound;
  for (;;) {
    const uint64_t value = splitmix64(state);
    if (value >= threshold) {
      return value % bound;
    }
  }
}

static uint64_t request_size(const struct options *options, uint64_t *rng) {
  switch (options->size_mode) {
  case SIZE_FIXED:
    return options->fixed_size;
  case SIZE_PM10: {
    const uint64_t minimum = options->fixed_size * 9 / 10;
    const uint64_t maximum = options->fixed_size * 11 / 10;
    return minimum + random_bounded(rng, maximum - minimum + 1);
  }
  case SIZE_UNIFORM:
    return options->minimum_size +
           random_bounded(rng,
                          options->maximum_size - options->minimum_size + 1);
  }
  return 0;
}

static int compare_u64(const void *left, const void *right) {
  const uint64_t lhs = *(const uint64_t *)left;
  const uint64_t rhs = *(const uint64_t *)right;
  return lhs < rhs ? -1 : lhs > rhs;
}

static void publish_error(atomic_int *shared_error, int error_number) {
  if (error_number == 0) {
    error_number = EIO;
  }
  int expected = 0;
  (void)atomic_compare_exchange_strong(shared_error, &expected, error_number);
}

static int checked_pwrite(int fd, const void *buffer, size_t length,
                          uint64_t offset) {
  const ssize_t completed = pwrite(fd, buffer, length, (off_t)offset);
  return completed == (ssize_t)length ? 0 : completed < 0 ? errno : EIO;
}

static int prepare_one(const struct options *options, unsigned file,
                       uint64_t *rng) {
  char path[4096];
  const int written = snprintf(path, sizeof(path), "%s-%u", options->path_prefix,
                               file);
  if (written < 0 || (size_t)written >= sizeof(path)) {
    return ENAMETOOLONG;
  }
  const int fd = open(path, O_CREAT | O_RDWR | O_TRUNC, 0600);
  if (fd < 0) {
    return errno;
  }
  if (!options->allow_host_fs && fd < ORCHFS_FD_OFFSET) {
    fprintf(stderr,
            "path %s opened as host fd %d; use a relative OrchFS path and "
            "the intended LD_PRELOAD library (or --allow-host-fs for a "
            "deliberate host-only generator test)\n",
            path, fd);
    close(fd);
    return EXDEV;
  }

  size_t maximum = (size_t)options->maximum_size;
  if (maximum < 4096) {
    maximum = 4096;
  }
  void *buffer = NULL;
  if (posix_memalign(&buffer, 4096, maximum + 4096) != 0) {
    close(fd);
    return ENOMEM;
  }
  memset(buffer, (int)((file + 1) & 0xffU), maximum + 4096);

  int error_number = 0;
  uint64_t offset = 0;
  while (offset < options->prepare_bytes) {
    uint64_t length = request_size(options, rng);
    if (length > options->prepare_bytes - offset) {
      length = options->prepare_bytes - offset;
    }
    error_number = checked_pwrite(fd, buffer, (size_t)length, offset);
    if (error_number != 0) {
      fprintf(stderr,
              "orchfs_repro_error label=%s file=%u stage=prepare_pwrite "
              "errno=%d offset=%" PRIu64 " length=%" PRIu64 "\n",
              options->label, file, error_number, offset, length);
      break;
    }
    offset += length;
  }
  if (error_number == 0 && fsync(fd) != 0) {
    error_number = errno;
    fprintf(stderr,
            "orchfs_repro_error label=%s file=%u stage=prepare_fsync "
            "errno=%d\n",
            options->label, file, error_number);
  }
  if (close(fd) != 0 && error_number == 0) {
    error_number = errno;
  }
  free(buffer);
  return error_number;
}

static int prepare_files(const struct options *options) {
  if (!options->prepare_requested) {
    return 0;
  }
  uint64_t rng = options->seed ^ 0x6a09e667f3bcc909ULL;
  const uint64_t started = now_ns();
  for (unsigned file = 0; file < options->files; ++file) {
    const int error_number = prepare_one(options, file, &rng);
    if (error_number != 0) {
      fprintf(stderr, "prepare file %u failed: %s\n", file,
              strerror(error_number));
      return error_number;
    }
  }
  const double seconds = (double)(now_ns() - started) / 1e9;
  printf("orchfs_repro_prepare label=%s files=%u bytes_per_file=%" PRIu64
         " seconds=%.9f MiB_per_s=%.3f\n",
         options->label, options->files, options->prepare_bytes, seconds,
         seconds > 0.0
             ? ((double)options->prepare_bytes * options->files / 1048576.0 /
                seconds)
             : 0.0);
  fflush(stdout);
  return 0;
}

static uint64_t choose_offset(struct worker *worker, uint64_t length,
                              uint64_t sequential_cursor) {
  const struct options *options = worker->options;
  if (options->access == ACCESS_APPEND) {
    return options->prepare_bytes + sequential_cursor;
  }
  if (options->access == ACCESS_SEQUENTIAL) {
    if (options->prepare_bytes <= length) {
      return 0;
    }
    return sequential_cursor % (options->prepare_bytes - length + 1);
  }

  if (options->prepare_bytes <= length) {
    return 0;
  }
  const uint64_t maximum = options->prepare_bytes - length;
  const uint64_t alignment = options->offset_alignment;
  uint64_t offset = alignment <= 1
                        ? random_bounded(&worker->rng, maximum + 1)
                        : random_bounded(&worker->rng, maximum / alignment + 1) *
                              alignment;
  if (options->unaligned_to > 1 && offset % options->unaligned_to == 0) {
    if (offset < maximum) {
      ++offset;
    } else if (offset > 0) {
      --offset;
    }
  }
  return offset;
}

static void record_series(struct worker *worker, uint64_t completed_ns,
                          uint64_t bytes) {
  struct series_state *series = worker->series;
  if (series == NULL || completed_ns < series->started_ns) {
    return;
  }
  size_t slot = (size_t)((completed_ns - series->started_ns) /
                         series->interval_ns);
  if (slot >= series->slots) {
    slot = series->slots - 1;
  }
  atomic_fetch_add_explicit(&series->bytes[slot], bytes,
                            memory_order_relaxed);
  atomic_fetch_add_explicit(&series->operations[slot], 1,
                            memory_order_relaxed);
}

static void *worker_main(void *opaque) {
  struct worker *worker = opaque;
  struct options *options = worker->options;
  size_t maximum = (size_t)options->maximum_size;
  if (maximum < 4096) {
    maximum = 4096;
  }
  void *allocation = NULL;
  if (posix_memalign(&allocation, 4096, maximum + 4096) != 0) {
    publish_error(worker->shared_error, ENOMEM);
    pthread_barrier_wait(worker->barrier);
    return NULL;
  }
  memset(allocation, (int)((worker->id + 1) & 0xffU), maximum + 4096);
  unsigned char *buffer = allocation;
  if (options->unaligned_to > 1) {
    ++buffer;
  }

  pthread_barrier_wait(worker->barrier);
  if (atomic_load_explicit(worker->shared_error, memory_order_acquire) != 0) {
    free(allocation);
    return NULL;
  }

  const uint64_t started = worker->series != NULL
                               ? worker->series->started_ns
                               : now_ns();
  const uint64_t deadline = options->duration_seconds > 0.0
                                ? started + (uint64_t)(options->duration_seconds *
                                                       1000000000.0)
                                : UINT64_MAX;
  uint64_t cursor = 0;
  while (atomic_load_explicit(worker->shared_error, memory_order_relaxed) == 0) {
    if (options->duration_seconds > 0.0) {
      if (now_ns() >= deadline) {
        break;
      }
    } else if (worker->bytes >= options->bytes_per_thread) {
      break;
    }

    uint64_t length = request_size(options, &worker->rng);
    if (options->duration_seconds <= 0.0 &&
        length > options->bytes_per_thread - worker->bytes) {
      length = options->bytes_per_thread - worker->bytes;
    }
    if (length == 0 || length > SIZE_MAX) {
      publish_error(worker->shared_error, EINVAL);
      break;
    }
    if (options->access != ACCESS_APPEND &&
        options->prepare_bytes < length) {
      publish_error(worker->shared_error, EINVAL);
      break;
    }
    const uint64_t offset = choose_offset(worker, length, cursor);
    const uint64_t operation_started = now_ns();
    const ssize_t completed = options->operation == OP_WRITE
                                  ? pwrite(worker->fd, buffer, (size_t)length,
                                           (off_t)offset)
                                  : pread(worker->fd, buffer, (size_t)length,
                                          (off_t)offset);
    int error_number = completed == (ssize_t)length
                           ? 0
                           : completed < 0 ? errno : EIO;
    if (error_number == 0 && options->operation == OP_WRITE &&
        options->sync_mode == SYNC_EACH && fsync(worker->fd) != 0) {
      error_number = errno;
      fprintf(stderr,
              "orchfs_repro_error label=%s thread=%u stage=fsync "
              "errno=%d offset=%" PRIu64 " length=%" PRIu64 "\n",
              options->label, worker->id, error_number, offset, length);
    }
    const uint64_t completed_ns = now_ns();
    if (error_number != 0) {
      if (completed != (ssize_t)length) {
        fprintf(stderr,
                "orchfs_repro_error label=%s thread=%u stage=%s "
                "errno=%d offset=%" PRIu64 " length=%" PRIu64
                " completed=%zd\n",
                options->label, worker->id,
                options->operation == OP_WRITE ? "pwrite" : "pread",
                error_number, offset, length, completed);
      }
      publish_error(worker->shared_error, error_number);
      break;
    }

    const uint64_t latency = completed_ns - operation_started;
    worker->latency_sum_ns += latency;
    if (worker->latency_count < worker->latency_capacity) {
      worker->latencies[worker->latency_count++] = latency;
    }
    worker->bytes += length;
    ++worker->operations;
    cursor += length;
    if (options->operation == OP_READ) {
      worker->checksum += buffer[(worker->operations * 131U) % length];
    }
    record_series(worker, completed_ns, length);
  }

  if (atomic_load_explicit(worker->shared_error, memory_order_relaxed) == 0 &&
      options->operation == OP_WRITE && options->sync_mode == SYNC_END &&
      fsync(worker->fd) != 0) {
    publish_error(worker->shared_error, errno);
  }
  free(allocation);
  return NULL;
}

static int open_worker_files(struct worker *workers, struct options *options) {
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    char path[4096];
    const unsigned file = thread % options->files;
    const int written = snprintf(path, sizeof(path), "%s-%u",
                                 options->path_prefix, file);
    if (written < 0 || (size_t)written >= sizeof(path)) {
      return ENAMETOOLONG;
    }
    workers[thread].fd = open(path, O_RDWR);
    if (workers[thread].fd < 0) {
      return errno;
    }
    if (!options->allow_host_fs && workers[thread].fd < ORCHFS_FD_OFFSET) {
      fprintf(stderr, "path %s opened as host fd %d during measurement\n",
              path, workers[thread].fd);
      return EXDEV;
    }
  }
  return 0;
}

static void close_worker_files(struct worker *workers,
                               const struct options *options) {
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    if (workers[thread].fd >= 0) {
      (void)close(workers[thread].fd);
      workers[thread].fd = -1;
    }
  }
}

static int write_series(const struct options *options,
                        const struct series_state *series) {
  if (series == NULL || options->series_path == NULL) {
    return 0;
  }
  FILE *output = fopen(options->series_path, "w");
  if (output == NULL) {
    return errno;
  }
  fprintf(output, "label,interval_start_s,interval_end_s,bytes,operations,MiB_per_s\n");
  const double interval_seconds = (double)series->interval_ns / 1e9;
  for (size_t slot = 0; slot < series->report_slots; ++slot) {
    const uint64_t bytes = atomic_load_explicit(&series->bytes[slot],
                                                memory_order_relaxed);
    const uint64_t operations = atomic_load_explicit(
        &series->operations[slot], memory_order_relaxed);
    fprintf(output, "%s,%.9f,%.9f,%" PRIu64 ",%" PRIu64 ",%.6f\n",
            options->label, slot * interval_seconds,
            (slot + 1) * interval_seconds, bytes, operations,
            (double)bytes / 1048576.0 / interval_seconds);
  }
  const int result = fclose(output);
  return result == 0 ? 0 : errno;
}

static int run_measurement(struct options *options) {
  if (options->bytes_per_thread == 0 && options->duration_seconds <= 0.0) {
    return 0;
  }
  struct worker *workers = calloc(options->threads, sizeof(*workers));
  if (workers == NULL) {
    return ENOMEM;
  }
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    workers[thread].fd = -1;
  }
  int error_number = open_worker_files(workers, options);
  if (error_number != 0) {
    close_worker_files(workers, options);
    free(workers);
    return error_number;
  }

  pthread_barrier_t barrier;
  if (pthread_barrier_init(&barrier, NULL, options->threads + 1) != 0) {
    close_worker_files(workers, options);
    free(workers);
    return EAGAIN;
  }
  atomic_int shared_error = 0;

  struct series_state series = {0};
  struct series_state *series_pointer = NULL;
  if (options->series_ms != 0) {
    const double expected_seconds = options->duration_seconds > 0.0
                                        ? options->duration_seconds
                                        : 3600.0;
    series.interval_ns = (uint64_t)options->series_ms * 1000000ULL;
    const double expected_intervals =
        expected_seconds * 1000.0 / options->series_ms;
    series.report_slots = (size_t)expected_intervals;
    if ((double)series.report_slots < expected_intervals) {
      ++series.report_slots;
    }
    /* An operation admitted immediately before the deadline may complete in
     * the following interval. Keep guard slots for lock-free recording, but
     * do not print those post-deadline buckets as zero-throughput samples. */
    series.slots = series.report_slots + 2;
    series.bytes = calloc(series.slots, sizeof(*series.bytes));
    series.operations = calloc(series.slots, sizeof(*series.operations));
    if (series.bytes == NULL || series.operations == NULL) {
      free(series.bytes);
      free(series.operations);
      pthread_barrier_destroy(&barrier);
      close_worker_files(workers, options);
      free(workers);
      return ENOMEM;
    }
    series_pointer = &series;
  }

  const size_t samples_per_thread =
      (options->latency_sample_limit + options->threads - 1) /
      options->threads;
  unsigned created = 0;
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    workers[thread].barrier = &barrier;
    workers[thread].shared_error = &shared_error;
    workers[thread].options = options;
    workers[thread].series = series_pointer;
    workers[thread].id = thread;
    workers[thread].rng = options->seed ^
                          (0xd1b54a32d192ed03ULL * (thread + 1));
    workers[thread].latency_capacity = samples_per_thread;
    if (samples_per_thread != 0) {
      workers[thread].latencies =
          malloc(samples_per_thread * sizeof(*workers[thread].latencies));
      if (workers[thread].latencies == NULL) {
        publish_error(&shared_error, ENOMEM);
      }
    }
    const int result = pthread_create(&workers[thread].thread, NULL,
                                      worker_main, &workers[thread]);
    if (result != 0) {
      fprintf(stderr, "pthread_create(%u): %s\n", thread, strerror(result));
      exit(2);
    }
    ++created;
  }

  const uint64_t started = now_ns();
  if (series_pointer != NULL) {
    series.started_ns = started;
  }
  pthread_barrier_wait(&barrier);
  for (unsigned thread = 0; thread < created; ++thread) {
    pthread_join(workers[thread].thread, NULL);
  }
  const uint64_t elapsed_ns = now_ns() - started;

  error_number = atomic_load_explicit(&shared_error, memory_order_acquire);
  uint64_t total_bytes = 0;
  uint64_t total_operations = 0;
  uint64_t total_latency_ns = 0;
  uint64_t checksum = 0;
  size_t total_samples = 0;
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    total_bytes += workers[thread].bytes;
    total_operations += workers[thread].operations;
    total_latency_ns += workers[thread].latency_sum_ns;
    checksum += workers[thread].checksum;
    total_samples += workers[thread].latency_count;
  }

  uint64_t *latencies = NULL;
  if (total_samples != 0) {
    latencies = malloc(total_samples * sizeof(*latencies));
    if (latencies == NULL) {
      error_number = error_number == 0 ? ENOMEM : error_number;
    } else {
      size_t cursor = 0;
      for (unsigned thread = 0; thread < options->threads; ++thread) {
        memcpy(latencies + cursor, workers[thread].latencies,
               workers[thread].latency_count * sizeof(*latencies));
        cursor += workers[thread].latency_count;
      }
      qsort(latencies, total_samples, sizeof(*latencies), compare_u64);
    }
  }

  if (error_number == 0) {
    error_number = write_series(options, series_pointer);
  }
  if (error_number == 0) {
    const double seconds = (double)elapsed_ns / 1e9;
    const double average_us = total_operations == 0
                                  ? 0.0
                                  : (double)total_latency_ns /
                                        total_operations / 1000.0;
    const double p50_us = latencies == NULL
                              ? 0.0
                              : (double)latencies[(total_samples - 1) / 2] /
                                    1000.0;
    const size_t p99_index = total_samples == 0
                                 ? 0
                                 : (total_samples * 99 + 99) / 100 - 1;
    const double p99_us = latencies == NULL
                              ? 0.0
                              : (double)latencies[p99_index] / 1000.0;
    printf("orchfs_repro_result label=%s operation=%s access=%s size_mode=%s "
           "fixed_size=%" PRIu64 " min_size=%" PRIu64
           " max_size=%" PRIu64 " threads=%u files=%u prepare_bytes=%" PRIu64
           " bytes=%" PRIu64 " operations=%" PRIu64
           " seconds=%.9f MiB_per_s=%.6f IOPS=%.6f avg_latency_us=%.6f "
           "p50_us=%.6f p99_us=%.6f latency_samples=%zu checksum=%" PRIu64
           " seed=%" PRIu64 "\n",
           options->label,
           options->operation == OP_WRITE ? "write" : "read",
           options->access == ACCESS_RANDOM
               ? "random"
               : options->access == ACCESS_SEQUENTIAL ? "sequential"
                                                       : "append",
           options->size_mode == SIZE_FIXED
               ? "fixed"
               : options->size_mode == SIZE_PM10 ? "pm10" : "uniform",
           options->fixed_size, options->minimum_size, options->maximum_size,
           options->threads, options->files, options->prepare_bytes,
           total_bytes, total_operations, seconds,
           seconds > 0.0 ? (double)total_bytes / 1048576.0 / seconds : 0.0,
           seconds > 0.0 ? (double)total_operations / seconds : 0.0,
           average_us, p50_us, p99_us, total_samples, checksum, options->seed);
    fflush(stdout);
  }

  for (unsigned thread = 0; thread < options->threads; ++thread) {
    free(workers[thread].latencies);
  }
  free(latencies);
  free(series.bytes);
  free(series.operations);
  pthread_barrier_destroy(&barrier);
  close_worker_files(workers, options);
  free(workers);
  return error_number;
}

static int cleanup_files(const struct options *options) {
  if (!options->cleanup) {
    return 0;
  }
  for (unsigned file = 0; file < options->files; ++file) {
    char path[4096];
    const int written = snprintf(path, sizeof(path), "%s-%u",
                                 options->path_prefix, file);
    if (written < 0 || (size_t)written >= sizeof(path)) {
      return ENAMETOOLONG;
    }
    if (unlink(path) != 0 && errno != ENOENT) {
      return errno;
    }
  }
  return 0;
}

int main(int argc, char **argv) {
  struct options options = {
      .operation = OP_WRITE,
      .access = ACCESS_RANDOM,
      .size_mode = SIZE_FIXED,
      .sync_mode = SYNC_EACH,
      .seed = 20260722,
      .fixed_size = 64 * 1024,
      .minimum_size = 64 * 1024,
      .maximum_size = 64 * 1024,
      .bytes_per_thread = 256 * 1024 * 1024ULL,
      .offset_alignment = 1,
      .threads = 1,
      .files = 1,
      .latency_sample_limit = 1000000,
      .label = "unnamed",
  };

  enum {
    OPT_PATH_PREFIX = 1000,
    OPT_OPERATION,
    OPT_ACCESS,
    OPT_SIZE_MODE,
    OPT_SIZE,
    OPT_MIN_SIZE,
    OPT_MAX_SIZE,
    OPT_PREPARE_BYTES,
    OPT_EXISTING_BYTES,
    OPT_BYTES_PER_THREAD,
    OPT_DURATION_SEC,
    OPT_THREADS,
    OPT_FILES,
    OPT_FSYNC,
    OPT_OFFSET_ALIGNMENT,
    OPT_UNALIGNED_TO,
    OPT_SEED,
    OPT_LABEL,
    OPT_SERIES_MS,
    OPT_SERIES_FILE,
    OPT_LATENCY_SAMPLES,
    OPT_CLEANUP,
    OPT_ALLOW_HOST_FS,
    OPT_HELP,
  };
  static const struct option long_options[] = {
      {"path-prefix", required_argument, NULL, OPT_PATH_PREFIX},
      {"operation", required_argument, NULL, OPT_OPERATION},
      {"access", required_argument, NULL, OPT_ACCESS},
      {"size-mode", required_argument, NULL, OPT_SIZE_MODE},
      {"size", required_argument, NULL, OPT_SIZE},
      {"min-size", required_argument, NULL, OPT_MIN_SIZE},
      {"max-size", required_argument, NULL, OPT_MAX_SIZE},
      {"prepare-bytes", required_argument, NULL, OPT_PREPARE_BYTES},
      {"existing-bytes", required_argument, NULL, OPT_EXISTING_BYTES},
      {"bytes-per-thread", required_argument, NULL, OPT_BYTES_PER_THREAD},
      {"duration-sec", required_argument, NULL, OPT_DURATION_SEC},
      {"threads", required_argument, NULL, OPT_THREADS},
      {"files", required_argument, NULL, OPT_FILES},
      {"fsync", required_argument, NULL, OPT_FSYNC},
      {"offset-alignment", required_argument, NULL, OPT_OFFSET_ALIGNMENT},
      {"unaligned-to", required_argument, NULL, OPT_UNALIGNED_TO},
      {"seed", required_argument, NULL, OPT_SEED},
      {"label", required_argument, NULL, OPT_LABEL},
      {"series-ms", required_argument, NULL, OPT_SERIES_MS},
      {"series-file", required_argument, NULL, OPT_SERIES_FILE},
      {"latency-samples", required_argument, NULL, OPT_LATENCY_SAMPLES},
      {"cleanup", no_argument, NULL, OPT_CLEANUP},
      {"allow-host-fs", no_argument, NULL, OPT_ALLOW_HOST_FS},
      {"help", no_argument, NULL, OPT_HELP},
      {NULL, 0, NULL, 0},
  };

  int option_index = 0;
  for (;;) {
    const int choice = getopt_long(argc, argv, "", long_options,
                                   &option_index);
    if (choice == -1) {
      break;
    }
    uint64_t parsed = 0;
    switch (choice) {
    case OPT_PATH_PREFIX:
      options.path_prefix = optarg;
      break;
    case OPT_OPERATION:
      if (strcmp(optarg, "read") == 0) {
        options.operation = OP_READ;
      } else if (strcmp(optarg, "write") == 0) {
        options.operation = OP_WRITE;
      } else {
        goto invalid;
      }
      break;
    case OPT_ACCESS:
      if (strcmp(optarg, "random") == 0) {
        options.access = ACCESS_RANDOM;
      } else if (strcmp(optarg, "sequential") == 0) {
        options.access = ACCESS_SEQUENTIAL;
      } else if (strcmp(optarg, "append") == 0) {
        options.access = ACCESS_APPEND;
      } else {
        goto invalid;
      }
      break;
    case OPT_SIZE_MODE:
      if (strcmp(optarg, "fixed") == 0) {
        options.size_mode = SIZE_FIXED;
      } else if (strcmp(optarg, "pm10") == 0) {
        options.size_mode = SIZE_PM10;
      } else if (strcmp(optarg, "uniform") == 0) {
        options.size_mode = SIZE_UNIFORM;
      } else {
        goto invalid;
      }
      break;
    case OPT_SIZE:
      if (!parse_u64(optarg, &options.fixed_size)) {
        goto invalid;
      }
      break;
    case OPT_MIN_SIZE:
      if (!parse_u64(optarg, &options.minimum_size)) {
        goto invalid;
      }
      break;
    case OPT_MAX_SIZE:
      if (!parse_u64(optarg, &options.maximum_size)) {
        goto invalid;
      }
      break;
    case OPT_PREPARE_BYTES:
      if (!parse_u64(optarg, &options.prepare_bytes)) {
        goto invalid;
      }
      options.prepare_requested = true;
      break;
    case OPT_EXISTING_BYTES:
      if (!parse_u64(optarg, &options.prepare_bytes)) {
        goto invalid;
      }
      options.prepare_requested = false;
      break;
    case OPT_BYTES_PER_THREAD:
      if (!parse_u64(optarg, &options.bytes_per_thread)) {
        goto invalid;
      }
      break;
    case OPT_DURATION_SEC:
      if (!parse_double(optarg, &options.duration_seconds)) {
        goto invalid;
      }
      break;
    case OPT_THREADS:
      if (!parse_unsigned(optarg, &options.threads)) {
        goto invalid;
      }
      break;
    case OPT_FILES:
      if (!parse_unsigned(optarg, &options.files)) {
        goto invalid;
      }
      break;
    case OPT_FSYNC:
      if (strcmp(optarg, "none") == 0) {
        options.sync_mode = SYNC_NONE;
      } else if (strcmp(optarg, "each") == 0) {
        options.sync_mode = SYNC_EACH;
      } else if (strcmp(optarg, "end") == 0) {
        options.sync_mode = SYNC_END;
      } else {
        goto invalid;
      }
      break;
    case OPT_OFFSET_ALIGNMENT:
      if (!parse_u64(optarg, &options.offset_alignment)) {
        goto invalid;
      }
      break;
    case OPT_UNALIGNED_TO:
      if (!parse_u64(optarg, &options.unaligned_to)) {
        goto invalid;
      }
      break;
    case OPT_SEED:
      if (!parse_u64(optarg, &options.seed)) {
        goto invalid;
      }
      break;
    case OPT_LABEL:
      options.label = optarg;
      break;
    case OPT_SERIES_MS:
      if (!parse_unsigned(optarg, &options.series_ms)) {
        goto invalid;
      }
      break;
    case OPT_SERIES_FILE:
      options.series_path = optarg;
      break;
    case OPT_LATENCY_SAMPLES:
      if (!parse_u64(optarg, &parsed) || parsed > SIZE_MAX) {
        goto invalid;
      }
      options.latency_sample_limit = (size_t)parsed;
      break;
    case OPT_CLEANUP:
      options.cleanup = true;
      break;
    case OPT_ALLOW_HOST_FS:
      options.allow_host_fs = true;
      break;
    case OPT_HELP:
      usage(argv[0]);
      return 0;
    default:
      goto invalid;
    }
  }

  if (optind != argc || options.path_prefix == NULL || options.threads == 0 ||
      options.files == 0 || options.fixed_size == 0 ||
      options.offset_alignment == 0 ||
      (options.series_ms != 0 && options.series_path == NULL)) {
    goto invalid;
  }
  if (options.size_mode == SIZE_PM10) {
    options.minimum_size = options.fixed_size * 9 / 10;
    options.maximum_size = options.fixed_size * 11 / 10;
  } else if (options.size_mode == SIZE_FIXED) {
    options.minimum_size = options.fixed_size;
    options.maximum_size = options.fixed_size;
  }
  if (options.minimum_size == 0 ||
      options.maximum_size < options.minimum_size ||
      options.maximum_size > SIZE_MAX ||
      (options.operation == OP_READ && options.access == ACCESS_APPEND)) {
    goto invalid;
  }

  int error_number = prepare_files(&options);
  if (error_number == 0) {
    error_number = run_measurement(&options);
  }
  if (error_number == 0) {
    error_number = cleanup_files(&options);
  }
  if (error_number != 0) {
    errno = error_number;
    perror("orchfs_repro_bench");
    return 1;
  }
  return 0;

invalid:
  usage(argv[0]);
  return 2;
}
