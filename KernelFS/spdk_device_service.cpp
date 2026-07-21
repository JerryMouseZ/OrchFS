#include "spdk_device_service.h"

#include "spdk_nvme_bridge.h"

#include "orchfs/async/runtime.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <sched.h>

namespace {

constexpr std::size_t kDefaultPollerCount = 4;
constexpr std::uint64_t kPollerStripeBytes = 32U * 1024U;

struct LoadedConfig {
  orchfs_spdk_config bridge{};
  std::string pci_bdf;
  std::string reactor_mask;
  std::string hugepage_directory;
  std::vector<unsigned> poller_cpus;
};

template <typename Integer>
int parse_integer(const char *name, Integer minimum, Integer maximum,
                  Integer &value) noexcept {
  const char *text = std::getenv(name);
  if (text == nullptr || *text == '\0') {
    return 0;
  }
  Integer parsed{};
  const std::string_view input(text);
  const auto [end, error] =
      std::from_chars(input.data(), input.data() + input.size(), parsed);
  if (error != std::errc{} || end != input.data() + input.size() ||
      parsed < minimum || parsed > maximum) {
    return EINVAL;
  }
  value = parsed;
  return 0;
}

int available_cpus(std::vector<unsigned> &cpus) noexcept {
  cpu_set_t affinity;
  CPU_ZERO(&affinity);
  if (::sched_getaffinity(0, sizeof(affinity), &affinity) != 0) {
    return errno;
  }
  try {
    for (unsigned cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
      if (CPU_ISSET(cpu, &affinity)) {
        cpus.push_back(cpu);
      }
    }
  } catch (const std::bad_alloc &) {
    return ENOMEM;
  }
  return cpus.empty() ? EINVAL : 0;
}

int parse_cpu_list(std::string_view input,
                   std::vector<unsigned> &cpus) noexcept {
  try {
    std::size_t cursor = 0;
    while (cursor < input.size()) {
      const std::size_t comma = input.find(',', cursor);
      const std::string_view token = input.substr(
          cursor, comma == std::string_view::npos ? input.size() - cursor
                                                  : comma - cursor);
      if (token.empty()) {
        return EINVAL;
      }
      unsigned cpu{};
      const auto [end, error] =
          std::from_chars(token.data(), token.data() + token.size(), cpu);
      if (error != std::errc{} || end != token.data() + token.size() ||
          cpu >= CPU_SETSIZE) {
        return EINVAL;
      }
      if (std::find(cpus.begin(), cpus.end(), cpu) == cpus.end()) {
        cpus.push_back(cpu);
      }
      if (comma == std::string_view::npos) {
        break;
      }
      cursor = comma + 1;
    }
  } catch (const std::bad_alloc &) {
    return ENOMEM;
  }
  return cpus.empty() ? EINVAL : 0;
}

std::string cpu_mask(const std::vector<unsigned> &cpus) {
  const unsigned highest = *std::max_element(cpus.begin(), cpus.end());
  std::string digits(highest / 4 + 1, '0');
  constexpr char hexadecimal[] = "0123456789abcdef";
  for (const unsigned cpu : cpus) {
    const std::size_t index = digits.size() - 1 - cpu / 4;
    unsigned value = digits[index] <= '9'
                         ? static_cast<unsigned>(digits[index] - '0')
                         : static_cast<unsigned>(digits[index] - 'a' + 10);
    value |= 1U << (cpu % 4);
    digits[index] = hexadecimal[value];
  }
  return "0x" + digits;
}

int load_config(LoadedConfig &loaded,
                orchfs::async::Runtime *runtime) noexcept {
  try {
    orchfs_spdk_config_init(&loaded.bridge);

    const char *bdf = std::getenv("ORCHFS_SPDK_PCI_BDF");
    const char *nsid = std::getenv("ORCHFS_SPDK_NSID");
    if (bdf == nullptr || *bdf == '\0' || nsid == nullptr || *nsid == '\0') {
      return EINVAL;
    }
    loaded.pci_bdf = bdf;

    int error = 0;
    if (runtime != nullptr) {
      loaded.poller_cpus.reserve(runtime->worker_count());
      for (std::size_t worker = 0; worker < runtime->worker_count(); ++worker) {
        auto cpu = runtime->worker_cpu(worker);
        if (!cpu) {
          return EINVAL;
        }
        loaded.poller_cpus.push_back(cpu.value());
      }
    } else {
      const char *cpu_list = std::getenv("ORCHFS_SPDK_CPU_LIST");
      error = cpu_list != nullptr && *cpu_list != '\0'
                  ? parse_cpu_list(cpu_list, loaded.poller_cpus)
                  : available_cpus(loaded.poller_cpus);
      if (error != 0) {
        return error;
      }

      std::size_t poller_count =
          std::min(kDefaultPollerCount, loaded.poller_cpus.size());
      if ((error = parse_integer("ORCHFS_SPDK_POLLER_COUNT", std::size_t{1},
                                 loaded.poller_cpus.size(), poller_count)) !=
          0) {
        return error;
      }
      loaded.poller_cpus.resize(poller_count);
    }

    std::uint32_t namespace_id = 0;
    std::uint32_t queue_depth = 32;
    std::size_t bounce_buffers = 32;
    std::uint32_t max_transfer = 1024U * 1024U;
    int shared_memory_id = -1;
    if ((error = parse_integer("ORCHFS_SPDK_NSID", std::uint32_t{1},
                               std::numeric_limits<std::uint32_t>::max(),
                               namespace_id)) != 0 ||
        (error = parse_integer("ORCHFS_SPDK_QUEUE_DEPTH", std::uint32_t{1},
                               std::numeric_limits<std::uint32_t>::max(),
                               queue_depth)) != 0 ||
        (error = parse_integer("ORCHFS_SPDK_BOUNCE_BUFFERS", std::size_t{1},
                               std::numeric_limits<std::size_t>::max(),
                               bounce_buffers)) != 0 ||
        (error = parse_integer(
             "ORCHFS_SPDK_MAX_TRANSFER_SIZE", std::uint32_t{0},
             std::numeric_limits<std::uint32_t>::max(), max_transfer)) != 0 ||
        (error = parse_integer("ORCHFS_SPDK_SHM_ID", -1,
                               std::numeric_limits<int>::max(),
                               shared_memory_id)) != 0) {
      return error;
    }

    if (runtime != nullptr) {
      /* Runtime workers directly drive every qpair. Advertising all of them as
       * EAL lcores would launch idle DPDK workers that contend with the same
       * coroutine CPUs, so the environment gets one control lcore only. */
      const std::vector<unsigned> environment_cpu{loaded.poller_cpus.front()};
      loaded.reactor_mask = cpu_mask(environment_cpu);
    } else {
      const char *mask = std::getenv("ORCHFS_SPDK_REACTOR_MASK");
      loaded.reactor_mask = mask != nullptr && *mask != '\0'
          ? mask : cpu_mask(loaded.poller_cpus);
    }
    const char *hugepage = std::getenv("ORCHFS_SPDK_HUGEPAGE_DIR");
    if (hugepage != nullptr && *hugepage != '\0') {
      loaded.hugepage_directory = hugepage;
    }

    loaded.bridge.pci_bdf = loaded.pci_bdf.c_str();
    loaded.bridge.namespace_id = namespace_id;
    loaded.bridge.poller_count = loaded.poller_cpus.size();
    loaded.bridge.queue_depth = queue_depth;
    loaded.bridge.bounce_buffers_per_poller = bounce_buffers;
    loaded.bridge.max_transfer_size = max_transfer;
    loaded.bridge.reactor_mask = loaded.reactor_mask.c_str();
    loaded.bridge.hugepage_directory = loaded.hugepage_directory.empty()
                                           ? nullptr
                                           : loaded.hugepage_directory.c_str();
    loaded.bridge.shared_memory_id = shared_memory_id;
    return 0;
  } catch (const std::bad_alloc &) {
    return ENOMEM;
  } catch (...) {
    return EIO;
  }
}

void close_unowned_backend(orchfs_spdk_backend *backend) noexcept {
  if (backend == nullptr) {
    return;
  }
  orchfs_spdk_request_stop(backend);
  for (std::size_t index = 0; index < orchfs_spdk_poller_count(backend);
       ++index) {
    int stopped = 0;
    while (!stopped) {
      (void)orchfs_spdk_poll(backend, index, 0, nullptr, &stopped);
    }
  }
  (void)orchfs_spdk_close(backend);
}

class DeviceService;
thread_local DeviceService *current_poller_service = nullptr;

template <typename T>
class FixedObjectPool final {
public:
  explicit FixedObjectPool(std::size_t count)
      : storage_(std::make_unique<T[]>(count)) {
    free_.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      free_.push_back(&storage_[index]);
    }
  }

  T *acquire() noexcept {
    Guard guard(lock_);
    if (free_.empty()) {
      return nullptr;
    }
    T *object = free_.back();
    free_.pop_back();
    return object;
  }

  void release(T *object) noexcept {
    if (object == nullptr) {
      return;
    }
    *object = {};
    Guard guard(lock_);
    free_.push_back(object);
  }

private:
  class Guard final {
  public:
    explicit Guard(std::atomic_flag &lock) noexcept : lock_(lock) {
      while (lock_.test_and_set(std::memory_order_acquire)) {
      }
    }
    ~Guard() { lock_.clear(std::memory_order_release); }

  private:
    std::atomic_flag &lock_;
  };

  std::unique_ptr<T[]> storage_;
  std::vector<T *> free_;
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

class DeviceService final {
  enum class Lifecycle : std::uint8_t {
    running,
    stopping,
    stopped,
  };

public:
  static int create(std::shared_ptr<DeviceService> &service,
                    orchfs::async::Runtime *runtime = nullptr) noexcept {
    LoadedConfig config;
    int error = load_config(config, runtime);
    if (error != 0) {
      return error;
    }
    if (!orchfs_spdk_is_compiled()) {
      return ENOTSUP;
    }

    orchfs_spdk_backend *backend = nullptr;
    error = orchfs_spdk_open(&config.bridge, &backend);
    if (error != 0) {
      return error;
    }

    try {
      DeviceService *raw =
          new DeviceService(backend, std::move(config.poller_cpus),
                            config.bridge.queue_depth, runtime);
      /* raw owns backend from this point. shared_ptr's raw-pointer
       * constructor deletes raw if allocating its control block fails. */
      backend = nullptr;
      auto created = std::shared_ptr<DeviceService>(raw);
      error = created->start_pollers();
      if (error != 0) {
        (void)created->stop();
        return error;
      }
      service = std::move(created);
      return 0;
    } catch (const std::bad_alloc &) {
      close_unowned_backend(backend);
      return ENOMEM;
    } catch (...) {
      close_unowned_backend(backend);
      return EIO;
    }
  }

  ~DeviceService() { (void)stop(); }

  DeviceService(const DeviceService &) = delete;
  DeviceService &operator=(const DeviceService &) = delete;

  int submit_read(uint64_t offset, void *destination, size_t length,
                  orchfs_spdk_device_completion_fn callback,
                  void *context) noexcept {
    std::size_t poller = 0;
    const int owner_error = select_poller(offset, poller);
    if (owner_error != 0) {
      return owner_error;
    }
    return submit_admitted(
        poller, callback, context, [&](auto tracked, void *tracked_context) {
          return orchfs_spdk_submit_read(backend_, poller, offset, destination,
                                         length, tracked, tracked_context);
        });
  }

  int submit_write(uint64_t offset, const void *source, size_t length,
                   orchfs_spdk_device_completion_fn callback,
                   void *context) noexcept {
    std::size_t poller = 0;
    const int owner_error = select_poller(offset, poller);
    if (owner_error != 0) {
      return owner_error;
    }
    return submit_admitted(
        poller, callback, context, [&](auto tracked, void *tracked_context) {
          return orchfs_spdk_submit_write(backend_, poller, offset, source,
                                          length, tracked, tracked_context);
        });
  }

  int submit_flush(orchfs_spdk_device_completion_fn callback,
                   void *context) noexcept {
    std::size_t poller = 0;
    const int owner_error = select_poller(0, poller);
    if (owner_error != 0) {
      return owner_error;
    }
    return submit_admitted(poller, callback, context,
                           [&](auto tracked, void *tracked_context) {
                             return orchfs_spdk_submit_flush(
                                 backend_, poller, tracked, tracked_context);
                           });
  }

  int register_memory(void *address, size_t length) noexcept {
    if (address == nullptr || length == 0 || backend_ == nullptr ||
        !accepting_.load(std::memory_order_acquire)) {
      return EINVAL;
    }
    return orchfs_spdk_register_memory(backend_, address, length);
  }

  int unregister_memory(void *address, size_t length) noexcept {
    if (address == nullptr || length == 0 || backend_ == nullptr) {
      return EINVAL;
    }
    return orchfs_spdk_unregister_memory(backend_, address, length);
  }

  int stop() noexcept {
    if (current_poller_service == this) {
      return EDEADLK;
    }

    Lifecycle expected = Lifecycle::running;
    if (!lifecycle_.compare_exchange_strong(expected, Lifecycle::stopping,
                                            std::memory_order_acq_rel,
                                            std::memory_order_acquire)) {
      while (expected == Lifecycle::stopping) {
        lifecycle_.wait(expected, std::memory_order_acquire);
        expected = lifecycle_.load(std::memory_order_acquire);
      }
      return stop_result_.load(std::memory_order_acquire);
    }

    accepting_.store(false, std::memory_order_release);
    std::size_t active = active_submitters_.load(std::memory_order_acquire);
    while (active != 0) {
      active_submitters_.wait(active, std::memory_order_acquire);
      active = active_submitters_.load(std::memory_order_acquire);
    }

    orchfs_spdk_backend *backend = backend_;
    if (backend != nullptr) {
      orchfs_spdk_request_stop(backend);
    }

    int result = first_error_.load(std::memory_order_acquire);
    if (!pollers_joined_ && backend != nullptr) {
      if (runtime_ != nullptr) {
        std::size_t stopped =
            stopped_runtime_pollers_.load(std::memory_order_acquire);
        while (stopped != runtime_registered_pollers_) {
          stopped_runtime_pollers_.wait(stopped, std::memory_order_acquire);
          stopped = stopped_runtime_pollers_.load(std::memory_order_acquire);
        }
        runtime_registrations_.clear();
        for (std::size_t index = runtime_registered_pollers_;
             index < poller_cpus_.size(); ++index) {
          int stopped = 0;
          while (!stopped) {
            const int error =
                orchfs_spdk_poll(backend, index, 0, nullptr, &stopped);
            record_error(error);
            if (result == 0 && error != 0) {
              result = error;
            }
          }
        }
        pollers_joined_ = true;
      } else {
        // Standalone compatibility is caller-polled and creates no service
        // threads. The same caller that drove mkfs I/O drains every qpair.
        for (std::size_t index = 0; index < poller_cpus_.size(); ++index) {
          int stopped = 0;
          while (!stopped) {
            const int error =
                orchfs_spdk_poll(backend, index, 0, nullptr, &stopped);
            record_error(error);
            if (result == 0 && error != 0) {
              result = error;
            }
            if (error != 0) {
              break;
            }
          }
        }
        pollers_joined_ = true;
      }
    }

    int close_error = 0;
    if (pollers_joined_) {
      if (backend_ != nullptr) {
        close_error = orchfs_spdk_close(backend_);
        if (close_error == 0) {
          backend_ = nullptr;
        }
      }
    }
    if (result == 0) {
      result = close_error;
    }

    stop_result_.store(result, std::memory_order_release);
    fully_stopped_.store(pollers_joined_ && close_error == 0,
                         std::memory_order_release);
    lifecycle_.store(Lifecycle::stopped, std::memory_order_release);
    lifecycle_.notify_all();
    return result;
  }

  size_t poller_count() const noexcept { return poller_cpus_.size(); }
  uint32_t lba_size() const noexcept { return lba_size_; }
  uint64_t capacity_bytes() const noexcept { return capacity_bytes_; }
  bool fully_stopped() const noexcept {
    return fully_stopped_.load(std::memory_order_acquire);
  }

  bool runtime_integrated() const noexcept { return runtime_ != nullptr; }

  int poll_standalone_once() noexcept {
    if (runtime_integrated() || backend_ == nullptr) {
      return EINVAL;
    }
    struct PollerScope final {
      DeviceService *previous;
      explicit PollerScope(DeviceService *service) noexcept
          : previous(current_poller_service) {
        current_poller_service = service;
      }
      ~PollerScope() { current_poller_service = previous; }
    } poller_scope(this);

    int first_error = 0;
    for (std::size_t index = 0; index < poller_cpus_.size(); ++index) {
      std::size_t completed = 0;
      int stopped = 0;
      const int error =
          orchfs_spdk_poll(backend_, index, 0, &completed, &stopped);
      record_error(error);
      if (error != 0 && first_error == 0) {
        first_error = error;
      }
    }
    return first_error;
  }

private:
  struct TrackedCompletion {
    DeviceService *service{};
    std::size_t poller{};
    orchfs_spdk_device_completion_fn callback{};
    void *context{};
    FixedObjectPool<TrackedCompletion> *pool{};
  };

  static void release_tracked(TrackedCompletion *completion) noexcept {
    if (completion->pool != nullptr) {
      completion->pool->release(completion);
    } else {
      delete completion;
    }
  }

  static void complete_tracked(void *context, int error,
                               std::size_t bytes) noexcept {
    auto *completion = static_cast<TrackedCompletion *>(context);
    DeviceService *service = completion->service;
    const std::size_t poller = completion->poller;
    const auto callback = completion->callback;
    void *callback_context = completion->context;
    service->pending_[poller].fetch_sub(
        1, std::memory_order_acq_rel);
    release_tracked(completion);
    callback(callback_context, error, bytes);
  }

  template <typename Submit>
  int submit_tracked(std::size_t poller,
                     orchfs_spdk_device_completion_fn callback, void *context,
                     Submit &&submit) noexcept {
    if (callback == nullptr || poller >= poller_cpus_.size()) {
      return EINVAL;
    }
    auto *pool = completion_pools_[poller].get();
    TrackedCompletion *completion = pool->acquire();
    FixedObjectPool<TrackedCompletion> *release_pool = nullptr;
    if (completion != nullptr) {
      release_pool = pool;
    } else {
      completion = new (std::nothrow) TrackedCompletion();
    }
    if (completion == nullptr) {
      return ENOMEM;
    }
    *completion = TrackedCompletion{
        .service = this,
        .poller = poller,
        .callback = callback,
        .context = context,
        .pool = release_pool,
    };
    pending_[poller].fetch_add(1, std::memory_order_acq_rel);
    const int error = submit(&complete_tracked, completion);
    if (error != 0) {
      pending_[poller].fetch_sub(1, std::memory_order_acq_rel);
      release_tracked(completion);
      return error;
    }
    return 0;
  }

  template <typename Submit>
  int submit_admitted(std::size_t poller,
                      orchfs_spdk_device_completion_fn callback, void *context,
                      Submit &&submit) noexcept {
    if (!accepting_.load(std::memory_order_acquire)) {
      return ESHUTDOWN;
    }
    active_submitters_.fetch_add(1, std::memory_order_acq_rel);
    if (!accepting_.load(std::memory_order_acquire)) {
      if (active_submitters_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        active_submitters_.notify_all();
      }
      return ESHUTDOWN;
    }
    const int error = backend_ == nullptr
                          ? ENODEV
                          : submit_tracked(poller, callback, context,
                                           std::forward<Submit>(submit));
    if (active_submitters_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      active_submitters_.notify_all();
    }
    return error;
  }

  DeviceService(orchfs_spdk_backend *backend, std::vector<unsigned> cpus,
                std::uint32_t queue_depth,
                orchfs::async::Runtime *runtime)
      : backend_(backend), poller_cpus_(std::move(cpus)),
        pending_(
            std::make_unique<std::atomic<std::size_t>[]>(poller_cpus_.size())),
        lba_size_(orchfs_spdk_lba_size(backend)),
        capacity_bytes_(orchfs_spdk_capacity_bytes(backend)),
        runtime_(runtime) {
    const std::size_t completion_count =
        static_cast<std::size_t>(queue_depth) * 2U + 16U;
    completion_pools_.reserve(poller_cpus_.size());
    for (std::size_t index = 0; index < poller_cpus_.size(); ++index) {
      completion_pools_.push_back(
          std::make_unique<FixedObjectPool<TrackedCompletion>>(
              completion_count));
    }
  }

  void record_error(int error) noexcept {
    if (error == 0) {
      return;
    }
    int expected = 0;
    (void)first_error_.compare_exchange_strong(expected, error,
                                               std::memory_order_acq_rel);
  }

  int start_pollers() noexcept {
    if (runtime_ != nullptr) {
      return start_runtime_pollers();
    }
    // mkfs and other standalone callers drive completions synchronously
    // from their own thread. No permanent SPDK service thread is created.
    return 0;
  }

  struct RuntimePollerContext {
    DeviceService *service{};
    std::size_t index{};
    bool initialized{};
    bool stopped{};
  };

  static orchfs::async::Runtime::PollState runtime_poll(void *opaque) noexcept {
    auto &context = *static_cast<RuntimePollerContext *>(opaque);
    return context.service->poll_from_runtime(context);
  }

  orchfs::async::Runtime::PollState
  poll_from_runtime(RuntimePollerContext &context) noexcept {
    struct PollerScope final {
      DeviceService *previous;
      explicit PollerScope(DeviceService *service) noexcept
          : previous(current_poller_service) {
        current_poller_service = service;
      }
      ~PollerScope() { current_poller_service = previous; }
    } poller_scope(this);

    std::size_t completed = 0;
    int stopped = 0;
    const int error =
        orchfs_spdk_poll(backend_, context.index, 0, &completed, &stopped);
    record_error(error);
    if (error != 0) {
      accepting_.store(false, std::memory_order_release);
      orchfs_spdk_request_stop(backend_);
    }
    if (!context.initialized) {
      context.initialized = true;
      publish_ready();
    }
    if (stopped && !context.stopped) {
      context.stopped = true;
      stopped_runtime_pollers_.fetch_add(1, std::memory_order_acq_rel);
      stopped_runtime_pollers_.notify_all();
    }
    if (completed != 0) {
      return orchfs::async::Runtime::PollState::progress;
    }
    if (stopped) {
      return orchfs::async::Runtime::PollState::idle;
    }
    const bool pending =
        pending_[context.index].load(std::memory_order_acquire) != 0;
    const bool stopping = !accepting_.load(std::memory_order_acquire);
    return pending || stopping ? orchfs::async::Runtime::PollState::busy
                               : orchfs::async::Runtime::PollState::idle;
  }

  int start_runtime_pollers() noexcept {
    try {
      runtime_contexts_.reserve(poller_cpus_.size());
      runtime_registrations_.reserve(poller_cpus_.size());
      for (std::size_t index = 0; index < poller_cpus_.size(); ++index) {
        runtime_contexts_.push_back(RuntimePollerContext{
            .service = this,
            .index = index,
        });
        auto registered = runtime_->register_poller(index, &runtime_poll,
                                                    &runtime_contexts_.back());
        if (!registered) {
          record_error(registered.error().value() > 0
                           ? registered.error().value()
                           : EIO);
          accepting_.store(false, std::memory_order_release);
          orchfs_spdk_request_stop(backend_);
          return first_error_.load(std::memory_order_acquire);
        }
        runtime_registrations_.push_back(std::move(registered).value());
        ++runtime_registered_pollers_;
      }
    } catch (const std::bad_alloc &) {
      return ENOMEM;
    } catch (...) {
      return EIO;
    }

    std::size_t ready = ready_pollers_.load(std::memory_order_acquire);
    while (ready != poller_cpus_.size()) {
      ready_pollers_.wait(ready, std::memory_order_acquire);
      ready = ready_pollers_.load(std::memory_order_acquire);
    }
    return first_error_.load(std::memory_order_acquire);
  }
  void publish_ready() noexcept {
    ready_pollers_.fetch_add(1, std::memory_order_acq_rel);
    ready_pollers_.notify_all();
  }

  int select_poller(uint64_t offset, std::size_t &poller) const noexcept {
    if (runtime_ != nullptr) {
      if (orchfs::async::Runtime::current() != runtime_) {
        return EPERM;
      }
      poller = orchfs::async::Runtime::current_worker();
      return poller < poller_cpus_.size() ? 0 : EPERM;
    }
    poller = static_cast<std::size_t>((offset / kPollerStripeBytes) %
                                      poller_cpus_.size());
    return 0;
  }

  orchfs_spdk_backend *backend_{};
  std::vector<unsigned> poller_cpus_;
  std::unique_ptr<std::atomic<std::size_t>[]> pending_;
  std::vector<std::unique_ptr<FixedObjectPool<TrackedCompletion>>>
      completion_pools_;
  const uint32_t lba_size_{};
  const uint64_t capacity_bytes_{};
  std::atomic<bool> accepting_{true};
  std::atomic<std::size_t> active_submitters_{0};
  std::atomic<int> first_error_{0};

  std::atomic<std::size_t> ready_pollers_{0};
  std::atomic<Lifecycle> lifecycle_{Lifecycle::running};
  std::atomic<int> stop_result_{0};
  std::atomic<bool> fully_stopped_{false};
  bool pollers_joined_{};

  orchfs::async::Runtime *runtime_{};
  std::vector<RuntimePollerContext> runtime_contexts_;
  std::vector<orchfs::async::Runtime::PollRegistration> runtime_registrations_;
  std::size_t runtime_registered_pollers_{};
  std::atomic<std::size_t> stopped_runtime_pollers_{0};
};

std::atomic<std::shared_ptr<DeviceService>> global_service{};
std::atomic_flag global_lifecycle = ATOMIC_FLAG_INIT;

class LifecycleGuard final {
public:
  LifecycleGuard() noexcept {
    while (global_lifecycle.test_and_set(std::memory_order_acquire)) {
      global_lifecycle.wait(true, std::memory_order_relaxed);
    }
  }
  ~LifecycleGuard() {
    global_lifecycle.clear(std::memory_order_release);
    global_lifecycle.notify_all();
  }
};

std::shared_ptr<DeviceService> service_snapshot() noexcept {
  return global_service.load(std::memory_order_acquire);
}

#ifdef ORCHFS_FORMATTER
struct FormatterCompletion {
  std::atomic<bool> done{false};
  int error{};
  size_t bytes{};
};

void complete_formatter(void *context, int error, size_t bytes) noexcept {
    auto &completion = *static_cast<FormatterCompletion *>(context);
  completion.error = error;
  completion.bytes = bytes;
  completion.done.store(true, std::memory_order_release);
  completion.done.notify_one();
}

template <typename Submit>
int poll_formatter_completion(size_t expected_bytes, Submit &&submit) noexcept {
  if (current_poller_service != nullptr) {
    return EDEADLK;
  }
  auto service = service_snapshot();
  if (!service) {
    return ENODEV;
  }
    FormatterCompletion completion;
    const int submit_error =
        submit(&complete_formatter, static_cast<void *>(&completion));
  if (submit_error != 0) {
    return submit_error;
  }
  if (service->runtime_integrated()) {
    while (!completion.done.load(std::memory_order_acquire)) {
      completion.done.wait(false, std::memory_order_acquire);
    }
  } else {
    while (!completion.done.load(std::memory_order_acquire)) {
      const int poll_error = service->poll_standalone_once();
      if (poll_error != 0) {
        return poll_error;
      }
    }
  }
  if (completion.error != 0) {
    return completion.error;
  }
    return completion.bytes == expected_bytes ? 0 : EIO;
}
#endif

} // namespace

extern "C" {

int orchfs_spdk_device_start(void) {
  LifecycleGuard lifecycle;
  if (global_service.load(std::memory_order_acquire)) {
    return EALREADY;
  }
  std::shared_ptr<DeviceService> service;
  const int error = DeviceService::create(service);
  if (error == 0) {
    global_service.store(std::move(service), std::memory_order_release);
  }
  return error;
}

int orchfs_spdk_device_start_on_runtime(void *runtime_handle) {
  if (runtime_handle == nullptr) {
    return EINVAL;
  }
  LifecycleGuard lifecycle;
  if (global_service.load(std::memory_order_acquire)) {
    return EALREADY;
  }
  std::shared_ptr<DeviceService> service;
  const int error = DeviceService::create(
      service, static_cast<orchfs::async::Runtime *>(runtime_handle));
  if (error == 0) {
    global_service.store(std::move(service), std::memory_order_release);
  }
  return error;
}

int orchfs_spdk_device_stop(void) {
  if (current_poller_service != nullptr) {
    return EDEADLK;
  }
  LifecycleGuard lifecycle;
  auto service = global_service.exchange({}, std::memory_order_acq_rel);
  if (!service) {
    return 0;
  }
  const int error = service->stop();
  if (!service->fully_stopped()) {
    // Preserve the failed instance for a diagnostic/retry instead of
    // allowing a second controller owner to be opened concurrently.
    global_service.store(service, std::memory_order_release);
  }
  return error;
}

int orchfs_spdk_device_is_running(void) { return service_snapshot() ? 1 : 0; }

int orchfs_spdk_device_submit_read(uint64_t offset, void *destination,
                                   size_t length,
                                   orchfs_spdk_device_completion_fn callback,
                                   void *callback_context) {
  auto service = service_snapshot();
  return service ? service->submit_read(offset, destination, length, callback,
                                        callback_context)
                 : ENODEV;
}

int orchfs_spdk_device_submit_write(uint64_t offset, const void *source,
                                    size_t length,
                                    orchfs_spdk_device_completion_fn callback,
                                    void *callback_context) {
  auto service = service_snapshot();
  return service ? service->submit_write(offset, source, length, callback,
                                         callback_context)
                 : ENODEV;
}

int orchfs_spdk_device_submit_flush(orchfs_spdk_device_completion_fn callback,
                                    void *callback_context) {
  auto service = service_snapshot();
  return service ? service->submit_flush(callback, callback_context) : ENODEV;
}

int orchfs_spdk_device_register_memory(void *address, size_t length) {
  auto service = service_snapshot();
  return service ? service->register_memory(address, length) : ENODEV;
}

int orchfs_spdk_device_unregister_memory(void *address, size_t length) {
  auto service = service_snapshot();
  return service ? service->unregister_memory(address, length) : ENODEV;
}

#ifdef ORCHFS_FORMATTER
int orchfs_spdk_formatter_read(uint64_t offset, void *destination,
                              size_t length) {
    return poll_formatter_completion(length, [&](auto callback, void *context) {
    return orchfs_spdk_device_submit_read(offset, destination, length, callback,
                                          context);
  });
}

int orchfs_spdk_formatter_write(uint64_t offset, const void *source,
                               size_t length) {
    return poll_formatter_completion(length, [&](auto callback, void *context) {
    return orchfs_spdk_device_submit_write(offset, source, length, callback,
                                           context);
  });
}

int orchfs_spdk_formatter_flush(void) {
    return poll_formatter_completion(0, [&](auto callback, void *context) {
    return orchfs_spdk_device_submit_flush(callback, context);
    });
}
#endif

size_t orchfs_spdk_device_poller_count(void) {
  auto service = service_snapshot();
  return service ? service->poller_count() : 0;
}

uint32_t orchfs_spdk_device_lba_size(void) {
  auto service = service_snapshot();
  return service ? service->lba_size() : 0;
}

uint64_t orchfs_spdk_device_capacity_bytes(void) {
  auto service = service_snapshot();
  return service ? service->capacity_bytes() : 0;
}

} // extern "C"
