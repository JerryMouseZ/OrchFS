#define _GNU_SOURCE

#include <dirent.h>
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

enum operation_kind {
  OP_OPEN,
  OP_STAT,
  OP_LISTDIR,
  OP_MKDIR,
};

struct options {
  const char *path_prefix;
  const char *label;
  enum operation_kind operation;
  uint64_t operations_per_thread;
  size_t latency_sample_limit;
  unsigned threads;
  unsigned entries;
  bool allow_host_fs;
};

struct fixture {
  char *base;
  bool base_created;
  char *adapter_probe;
  bool adapter_probe_created;
  char **directories;
  size_t directory_capacity;
  size_t directory_count;
  char **files;
  size_t file_capacity;
  size_t file_count;
  char **mkdir_paths;
  size_t mkdir_path_count;
};

struct worker {
  pthread_t thread;
  pthread_barrier_t *start_barrier;
  pthread_barrier_t *finish_barrier;
  atomic_int *shared_error;
  const struct options *options;
  const struct fixture *fixture;
  unsigned id;
  uint64_t completed;
  uint64_t created_directories;
  uint64_t returned_items;
  uint64_t latency_sum_ns;
  uint64_t *latencies;
  size_t latency_capacity;
  size_t latency_count;
};

static void usage(const char *program) {
  fprintf(stderr,
          "usage: %s --path-prefix /Or/name --operation "
          "open|stat|listdir|mkdir [options]\n"
          "  --threads N --operations-per-thread N --entries N\n"
          "  --latency-samples N --label text --allow-host-fs\n",
          program);
}

static uint64_t now_ns(void) {
  struct timespec value;
  if (clock_gettime(CLOCK_MONOTONIC_RAW, &value) != 0) {
    return 0;
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
  const unsigned long long value = strtoull(text, &end, 10);
  if (errno != 0 || end == text || *end != '\0') {
    return false;
  }
  *output = (uint64_t)value;
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

static bool parse_size(const char *text, size_t *output) {
  uint64_t value = 0;
  if (!parse_u64(text, &value) || value > SIZE_MAX) {
    return false;
  }
  *output = (size_t)value;
  return true;
}

static const char *operation_name(enum operation_kind operation) {
  switch (operation) {
  case OP_OPEN:
    return "open";
  case OP_STAT:
    return "stat";
  case OP_LISTDIR:
    return "listdir";
  case OP_MKDIR:
    return "mkdir";
  }
  return "unknown";
}

static const char *operation_scope(enum operation_kind operation) {
  switch (operation) {
  case OP_OPEN:
    return "open+close";
  case OP_STAT:
    return "stat";
  case OP_LISTDIR:
    return "opendir+scan+closedir";
  case OP_MKDIR:
    return "mkdir";
  }
  return "unknown";
}

static char *indexed_path(const char *parent, const char *prefix,
                          uint64_t index) {
  char *path = NULL;
  if (asprintf(&path, "%s/%s-%" PRIu64, parent, prefix, index) < 0) {
    return NULL;
  }
  return path;
}

static int checked_close(int fd) {
  return close(fd) == 0 ? 0 : errno;
}

static int verify_orchfs_fd(const struct options *options, int fd,
                            const char *path) {
  if (options->allow_host_fs || fd >= ORCHFS_FD_OFFSET) {
    return 0;
  }
  fprintf(stderr,
          "path %s opened as host fd %d; load the intended OrchFS adapter "
          "or pass --allow-host-fs for a deliberate host-only smoke test\n",
          path, fd);
  return EXDEV;
}

static int fixture_cleanup(struct fixture *fixture) {
  int first_error = 0;
  if (fixture->adapter_probe != NULL) {
    if (fixture->adapter_probe_created && unlink(fixture->adapter_probe) != 0 &&
        errno != ENOENT) {
      first_error = errno;
    }
    free(fixture->adapter_probe);
    fixture->adapter_probe = NULL;
  }
  if (fixture->mkdir_paths != NULL) {
    for (size_t index = 0; index < fixture->mkdir_path_count; ++index) {
      free(fixture->mkdir_paths[index]);
    }
    free(fixture->mkdir_paths);
    fixture->mkdir_paths = NULL;
  }
  if (fixture->files != NULL) {
    for (size_t index = 0; index < fixture->file_capacity; ++index) {
      if (fixture->files[index] == NULL) {
        continue;
      }
      if (index < fixture->file_count &&
          unlink(fixture->files[index]) != 0 && errno != ENOENT &&
          first_error == 0) {
        first_error = errno;
      }
      free(fixture->files[index]);
    }
    free(fixture->files);
    fixture->files = NULL;
  }
  if (fixture->directories != NULL) {
    for (size_t thread = 0; thread < fixture->directory_capacity; ++thread) {
      if (fixture->directories[thread] == NULL) {
        continue;
      }
      if (thread < fixture->directory_count &&
          rmdir(fixture->directories[thread]) != 0 && errno != ENOENT &&
          first_error == 0) {
        first_error = errno;
      }
      free(fixture->directories[thread]);
    }
    free(fixture->directories);
    fixture->directories = NULL;
  }
  if (fixture->base != NULL) {
    if (fixture->base_created && rmdir(fixture->base) != 0 &&
        errno != ENOENT && first_error == 0) {
      first_error = errno;
    }
    free(fixture->base);
    fixture->base = NULL;
  }
  return first_error;
}

static int fixture_create(const struct options *options,
                          struct fixture *fixture) {
  fixture->base = strdup(options->path_prefix);
  if (fixture->base == NULL) {
    return ENOMEM;
  }
  if (mkdir(fixture->base, 0700) != 0) {
    const int error_number = errno;
    fprintf(stderr, "metadata fixture root %s: %s\n", fixture->base,
            strerror(error_number));
    return error_number;
  }
  fixture->base_created = true;

  fixture->adapter_probe = indexed_path(fixture->base, "adapter-probe", 0);
  if (fixture->adapter_probe == NULL) {
    return ENOMEM;
  }
  const int probe_fd = open(fixture->adapter_probe,
                            O_CREAT | O_EXCL | O_RDWR, 0600);
  if (probe_fd < 0) {
    return errno;
  }
  fixture->adapter_probe_created = true;
  int probe_error = verify_orchfs_fd(
      options, probe_fd, fixture->adapter_probe);
  const int probe_close_error = checked_close(probe_fd);
  if (probe_error == 0) {
    probe_error = probe_close_error;
  }
  if (probe_error != 0) {
    return probe_error;
  }

  fixture->directory_capacity = options->threads;
  fixture->directories = calloc((size_t)options->threads + 1,
                                sizeof(*fixture->directories));
  if (fixture->directories == NULL) {
    return ENOMEM;
  }
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    fixture->directories[thread] = indexed_path(fixture->base, "thread",
                                                thread);
    if (fixture->directories[thread] == NULL) {
      return ENOMEM;
    }
    if (mkdir(fixture->directories[thread], 0700) != 0) {
      return errno;
    }
    ++fixture->directory_count;
  }

  if (options->operation == OP_MKDIR) {
    if (options->operations_per_thread > SIZE_MAX ||
        (size_t)options->operations_per_thread !=
            options->operations_per_thread ||
        (options->operations_per_thread != 0 &&
         options->threads > SIZE_MAX / options->operations_per_thread)) {
      return EOVERFLOW;
    }
    fixture->mkdir_path_count =
        (size_t)options->threads * (size_t)options->operations_per_thread;
    fixture->mkdir_paths = calloc(
        fixture->mkdir_path_count, sizeof(*fixture->mkdir_paths));
    if (fixture->mkdir_paths == NULL) {
      return ENOMEM;
    }
    for (unsigned thread = 0; thread < options->threads; ++thread) {
      for (uint64_t operation = 0;
           operation < options->operations_per_thread; ++operation) {
        const size_t index =
            (size_t)thread * (size_t)options->operations_per_thread +
            (size_t)operation;
        fixture->mkdir_paths[index] = indexed_path(
            fixture->directories[thread], "created", operation);
        if (fixture->mkdir_paths[index] == NULL) {
          return ENOMEM;
        }
      }
    }
    return 0;
  }
  if (options->entries != 0 &&
      options->threads > SIZE_MAX / options->entries) {
    return EOVERFLOW;
  }
  fixture->file_capacity = (size_t)options->threads * options->entries;
  fixture->files = calloc(fixture->file_capacity, sizeof(*fixture->files));
  if (fixture->files == NULL) {
    return ENOMEM;
  }
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    for (unsigned entry = 0; entry < options->entries; ++entry) {
      const size_t index = (size_t)thread * options->entries + entry;
      fixture->files[index] = indexed_path(
          fixture->directories[thread], "entry", entry);
      if (fixture->files[index] == NULL) {
        return ENOMEM;
      }
      const int fd = open(fixture->files[index],
                          O_CREAT | O_EXCL | O_RDWR, 0600);
      if (fd < 0) {
        return errno;
      }
      ++fixture->file_count;
      int error_number = verify_orchfs_fd(
          options, fd, fixture->files[index]);
      const int close_error = checked_close(fd);
      if (error_number == 0) {
        error_number = close_error;
      }
      if (error_number != 0) {
        return error_number;
      }
    }
  }
  return 0;
}

static void publish_error(atomic_int *shared_error, int error_number) {
  if (error_number == 0) {
    error_number = EIO;
  }
  int expected = 0;
  (void)atomic_compare_exchange_strong(shared_error, &expected,
                                       error_number);
}

static const char *worker_file(const struct worker *worker,
                               uint64_t iteration) {
  const struct options *options = worker->options;
  const unsigned entry = (unsigned)((iteration * 17 + worker->id) %
                                    options->entries);
  return worker->fixture->files[(size_t)worker->id * options->entries +
                                entry];
}

static int run_open(struct worker *worker, uint64_t iteration) {
  const char *path = worker_file(worker, iteration);
  const int fd = open(path, O_RDONLY);
  if (fd < 0) {
    return errno;
  }
  int error_number = verify_orchfs_fd(worker->options, fd, path);
  const int close_error = checked_close(fd);
  if (error_number == 0) {
    error_number = close_error;
  }
  return error_number;
}

static int run_stat(struct worker *worker, uint64_t iteration) {
  struct stat value;
  if (stat(worker_file(worker, iteration), &value) != 0) {
    return errno;
  }
  return S_ISREG(value.st_mode) ? 0 : EIO;
}

static int run_listdir(struct worker *worker) {
  DIR *directory = opendir(worker->fixture->directories[worker->id]);
  if (directory == NULL) {
    return errno;
  }
  uint64_t count = 0;
  errno = 0;
  while (readdir(directory) != NULL) {
    ++count;
  }
  int error_number = errno;
  if (closedir(directory) != 0 && error_number == 0) {
    error_number = errno;
  }
  if (error_number == 0 && count < worker->options->entries) {
    error_number = EIO;
  }
  worker->returned_items += count;
  return error_number;
}

static int run_mkdir(struct worker *worker, uint64_t iteration) {
  const size_t index =
      (size_t)worker->id *
          (size_t)worker->options->operations_per_thread +
      (size_t)iteration;
  const char *path = worker->fixture->mkdir_paths[index];
  const int result = mkdir(path, 0700);
  const int error_number = result == 0 ? 0 : errno;
  if (error_number == 0) {
    ++worker->created_directories;
    ++worker->returned_items;
  }
  return error_number;
}

static int run_operation(struct worker *worker, uint64_t iteration) {
  switch (worker->options->operation) {
  case OP_OPEN:
    return run_open(worker, iteration);
  case OP_STAT:
    return run_stat(worker, iteration);
  case OP_LISTDIR:
    return run_listdir(worker);
  case OP_MKDIR:
    return run_mkdir(worker, iteration);
  }
  return EINVAL;
}

static void *worker_main(void *opaque) {
  struct worker *worker = opaque;
  pthread_barrier_wait(worker->start_barrier);
  for (uint64_t iteration = 0;
       iteration < worker->options->operations_per_thread; ++iteration) {
    if (atomic_load_explicit(worker->shared_error,
                             memory_order_relaxed) != 0) {
      break;
    }
    const uint64_t started = now_ns();
    const int error_number = run_operation(worker, iteration);
    const uint64_t ended = now_ns();
    if (started == 0 || ended < started) {
      publish_error(worker->shared_error, EIO);
      break;
    }
    if (error_number != 0) {
      publish_error(worker->shared_error, error_number);
      break;
    }
    const uint64_t latency = ended - started;
    worker->latency_sum_ns += latency;
    if (worker->latency_count < worker->latency_capacity) {
      worker->latencies[worker->latency_count++] = latency;
    }
    ++worker->completed;
  }
  pthread_barrier_wait(worker->finish_barrier);
  return NULL;
}

static int cleanup_created_directories(const struct worker *workers,
                                       const struct options *options) {
  int first_error = 0;
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    for (uint64_t index = 0; index < workers[thread].created_directories;
         ++index) {
      const char *path = workers[thread].fixture->mkdir_paths[
          (size_t)thread * (size_t)options->operations_per_thread +
          (size_t)index];
      if (rmdir(path) != 0 && errno != ENOENT && first_error == 0) {
        first_error = errno;
      }
    }
  }
  return first_error;
}

static int compare_u64(const void *left, const void *right) {
  const uint64_t lhs = *(const uint64_t *)left;
  const uint64_t rhs = *(const uint64_t *)right;
  return lhs < rhs ? -1 : lhs > rhs;
}

static double percentile(const uint64_t *values, size_t count,
                         unsigned numerator) {
  if (count == 0) {
    return 0.0;
  }
  size_t rank = (count * numerator + 99) / 100;
  if (rank == 0) {
    rank = 1;
  }
  return (double)values[rank - 1] / 1000.0;
}

static int run_benchmark(const struct options *options,
                         const struct fixture *fixture) {
  struct worker *workers = calloc(options->threads, sizeof(*workers));
  if (workers == NULL) {
    return ENOMEM;
  }
  pthread_barrier_t start_barrier;
  pthread_barrier_t finish_barrier;
  if (pthread_barrier_init(&start_barrier, NULL,
                           options->threads + 1) != 0) {
    free(workers);
    return EAGAIN;
  }
  if (pthread_barrier_init(&finish_barrier, NULL,
                           options->threads + 1) != 0) {
    pthread_barrier_destroy(&start_barrier);
    free(workers);
    return EAGAIN;
  }
  atomic_int shared_error = 0;
  const size_t samples_per_thread =
      options->latency_sample_limit / options->threads +
      (options->latency_sample_limit % options->threads != 0);
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    workers[thread].start_barrier = &start_barrier;
    workers[thread].finish_barrier = &finish_barrier;
    workers[thread].shared_error = &shared_error;
    workers[thread].options = options;
    workers[thread].fixture = fixture;
    workers[thread].id = thread;
    workers[thread].latency_capacity =
        options->operations_per_thread < samples_per_thread
            ? (size_t)options->operations_per_thread
            : samples_per_thread;
    if (workers[thread].latency_capacity != 0) {
      workers[thread].latencies = malloc(
          workers[thread].latency_capacity * sizeof(uint64_t));
      if (workers[thread].latencies == NULL) {
        fprintf(stderr, "metadata benchmark latency allocation failed\n");
        exit(2);
      }
    }
    const int create_error = pthread_create(
        &workers[thread].thread, NULL, worker_main, &workers[thread]);
    if (create_error != 0) {
      fprintf(stderr, "pthread_create(%u): %s\n", thread,
              strerror(create_error));
      exit(2);
    }
  }

  const uint64_t started = now_ns();
  pthread_barrier_wait(&start_barrier);
  pthread_barrier_wait(&finish_barrier);
  const uint64_t ended = now_ns();
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    pthread_join(workers[thread].thread, NULL);
  }

  uint64_t completed = 0;
  uint64_t items = 0;
  uint64_t latency_sum = 0;
  size_t latency_count = 0;
  for (unsigned thread = 0; thread < options->threads; ++thread) {
    completed += workers[thread].completed;
    items += workers[thread].returned_items;
    latency_sum += workers[thread].latency_sum_ns;
    latency_count += workers[thread].latency_count;
  }
  uint64_t *latencies = latency_count == 0
                            ? NULL
                            : malloc(latency_count * sizeof(*latencies));
  if (latency_count != 0 && latencies == NULL) {
    publish_error(&shared_error, ENOMEM);
  } else if (latency_count != 0) {
    size_t cursor = 0;
    for (unsigned thread = 0; thread < options->threads; ++thread) {
      memcpy(latencies + cursor, workers[thread].latencies,
             workers[thread].latency_count * sizeof(*latencies));
      cursor += workers[thread].latency_count;
    }
    qsort(latencies, latency_count, sizeof(*latencies), compare_u64);
  }

  int error_number = atomic_load_explicit(&shared_error,
                                           memory_order_acquire);
  const int child_cleanup = cleanup_created_directories(workers, options);
  if (error_number == 0) {
    error_number = child_cleanup;
  }
  if (error_number == 0 && (started == 0 || ended <= started)) {
    error_number = EIO;
  }
  if (error_number == 0) {
    const double seconds = (double)(ended - started) / 1e9;
    printf("orchfs_metadata_result label=%s operation=%s scope=%s "
           "threads=%u operations_per_thread=%" PRIu64
           " operations=%" PRIu64 " entries_per_directory=%u "
           "started_ns=%" PRIu64 " ended_ns=%" PRIu64 " "
           "seconds=%.9f ops_per_s=%.3f mean_us=%.3f p50_us=%.3f "
           "p99_us=%.3f returned_items=%" PRIu64
           " items_per_operation=%.3f\n",
           options->label, operation_name(options->operation),
           operation_scope(options->operation), options->threads,
           options->operations_per_thread, completed, options->entries,
           started, ended, seconds, completed / seconds,
           completed == 0 ? 0.0 : (double)latency_sum / completed / 1000.0,
           percentile(latencies, latency_count, 50),
           percentile(latencies, latency_count, 99), items,
           completed == 0 ? 0.0 : (double)items / completed);
    fflush(stdout);
  } else {
    fprintf(stderr,
            "orchfs_metadata_error label=%s operation=%s threads=%u "
            "completed=%" PRIu64 " errno=%d message=%s\n",
            options->label, operation_name(options->operation),
            options->threads, completed, error_number,
            strerror(error_number));
  }

  for (unsigned thread = 0; thread < options->threads; ++thread) {
    free(workers[thread].latencies);
  }
  free(latencies);
  pthread_barrier_destroy(&finish_barrier);
  pthread_barrier_destroy(&start_barrier);
  free(workers);
  return error_number;
}

int main(int argc, char **argv) {
  struct options options = {
      .label = "metadata",
      .operation = OP_OPEN,
      .operations_per_thread = 1000,
      .latency_sample_limit = 100000,
      .threads = 1,
      .entries = 64,
  };
  bool operation_set = false;
  static const struct option long_options[] = {
      {"path-prefix", required_argument, NULL, 'p'},
      {"operation", required_argument, NULL, 'o'},
      {"threads", required_argument, NULL, 't'},
      {"operations-per-thread", required_argument, NULL, 'n'},
      {"entries", required_argument, NULL, 'e'},
      {"latency-samples", required_argument, NULL, 's'},
      {"label", required_argument, NULL, 'l'},
      {"allow-host-fs", no_argument, NULL, 'H'},
      {"help", no_argument, NULL, 'h'},
      {NULL, 0, NULL, 0},
  };
  for (;;) {
    const int option = getopt_long(argc, argv, "p:o:t:n:e:s:l:Hh",
                                   long_options, NULL);
    if (option == -1) {
      break;
    }
    switch (option) {
    case 'p':
      options.path_prefix = optarg;
      break;
    case 'o':
      operation_set = true;
      if (strcmp(optarg, "open") == 0) {
        options.operation = OP_OPEN;
      } else if (strcmp(optarg, "stat") == 0) {
        options.operation = OP_STAT;
      } else if (strcmp(optarg, "listdir") == 0) {
        options.operation = OP_LISTDIR;
      } else if (strcmp(optarg, "mkdir") == 0) {
        options.operation = OP_MKDIR;
      } else {
        usage(argv[0]);
        return 2;
      }
      break;
    case 't':
      if (!parse_unsigned(optarg, &options.threads)) {
        usage(argv[0]);
        return 2;
      }
      break;
    case 'n':
      if (!parse_u64(optarg, &options.operations_per_thread)) {
        usage(argv[0]);
        return 2;
      }
      break;
    case 'e':
      if (!parse_unsigned(optarg, &options.entries)) {
        usage(argv[0]);
        return 2;
      }
      break;
    case 's':
      if (!parse_size(optarg, &options.latency_sample_limit)) {
        usage(argv[0]);
        return 2;
      }
      break;
    case 'l':
      options.label = optarg;
      break;
    case 'H':
      options.allow_host_fs = true;
      break;
    case 'h':
      usage(argv[0]);
      return 0;
    default:
      usage(argv[0]);
      return 2;
    }
  }
  if (optind != argc || options.path_prefix == NULL || !operation_set ||
      options.threads == 0 || options.threads > 1024 ||
      options.operations_per_thread == 0 ||
      (options.operation != OP_MKDIR && options.entries == 0)) {
    usage(argv[0]);
    return 2;
  }
  if (!options.allow_host_fs &&
      strncmp(options.path_prefix, "/Or/", 4) != 0) {
    fprintf(stderr,
            "metadata benchmark path must be below /Or unless "
            "--allow-host-fs is set\n");
    return 2;
  }

  struct fixture fixture = {0};
  int error_number = fixture_create(&options, &fixture);
  if (error_number == 0) {
    error_number = run_benchmark(&options, &fixture);
  } else {
    fprintf(stderr,
            "orchfs_metadata_error label=%s operation=%s threads=%u "
            "completed=0 errno=%d message=%s\n",
            options.label, operation_name(options.operation), options.threads,
            error_number, strerror(error_number));
  }
  const int cleanup_error = fixture_cleanup(&fixture);
  if (error_number == 0) {
    error_number = cleanup_error;
  }
  return error_number == 0 ? 0 : 1;
}
