#include "spdk_device_service.h"

#include "spdk_nvme_bridge.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <condition_variable>
#include <cstdlib>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <pthread.h>
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

int parse_cpu_list(std::string_view input, std::vector<unsigned> &cpus) noexcept {
    try {
        std::size_t cursor = 0;
        while (cursor < input.size()) {
            const std::size_t comma = input.find(',', cursor);
            const std::string_view token = input.substr(
                cursor, comma == std::string_view::npos
                            ? input.size() - cursor
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

int load_config(LoadedConfig &loaded) noexcept {
    try {
        orchfs_spdk_config_init(&loaded.bridge);

        const char *bdf = std::getenv("ORCHFS_SPDK_PCI_BDF");
        const char *nsid = std::getenv("ORCHFS_SPDK_NSID");
        if (bdf == nullptr || *bdf == '\0' || nsid == nullptr ||
            *nsid == '\0') {
            return EINVAL;
        }
        loaded.pci_bdf = bdf;

        const char *cpu_list = std::getenv("ORCHFS_SPDK_CPU_LIST");
        int error = cpu_list != nullptr && *cpu_list != '\0'
                        ? parse_cpu_list(cpu_list, loaded.poller_cpus)
                        : available_cpus(loaded.poller_cpus);
        if (error != 0) {
            return error;
        }

        std::size_t poller_count =
            std::min(kDefaultPollerCount, loaded.poller_cpus.size());
        if ((error = parse_integer("ORCHFS_SPDK_POLLER_COUNT", std::size_t{1},
                                   loaded.poller_cpus.size(), poller_count)) != 0) {
            return error;
        }
        loaded.poller_cpus.resize(poller_count);

        std::uint32_t namespace_id = 0;
        std::uint32_t queue_depth = 128;
        std::size_t bounce_buffers = 64;
        std::uint32_t max_transfer = 0;
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
            (error = parse_integer("ORCHFS_SPDK_MAX_TRANSFER_SIZE",
                                   std::uint32_t{0},
                                   std::numeric_limits<std::uint32_t>::max(),
                                   max_transfer)) != 0 ||
            (error = parse_integer("ORCHFS_SPDK_SHM_ID", -1,
                                   std::numeric_limits<int>::max(),
                                   shared_memory_id)) != 0) {
            return error;
        }

        const char *mask = std::getenv("ORCHFS_SPDK_REACTOR_MASK");
        loaded.reactor_mask = mask != nullptr && *mask != '\0'
                                  ? mask
                                  : cpu_mask(loaded.poller_cpus);
        const char *hugepage = std::getenv("ORCHFS_SPDK_HUGEPAGE_DIR");
        if (hugepage != nullptr && *hugepage != '\0') {
            loaded.hugepage_directory = hugepage;
        }

        loaded.bridge.pci_bdf = loaded.pci_bdf.c_str();
        loaded.bridge.namespace_id = namespace_id;
        loaded.bridge.poller_count = poller_count;
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

class DeviceService final {
  public:
    static int create(std::shared_ptr<DeviceService> &service) noexcept {
        LoadedConfig config;
        int error = load_config(config);
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
                new DeviceService(backend, std::move(config.poller_cpus));
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
        std::lock_guard backend_lock(backend_mutex_);
        if (!accepting_.load(std::memory_order_acquire)) {
            return ESHUTDOWN;
        }
        if (backend_ == nullptr) {
            return ENODEV;
        }
        return orchfs_spdk_submit_read(backend_, poller_for(offset), offset,
                                      destination, length, callback, context);
    }

    int submit_write(uint64_t offset, const void *source, size_t length,
                     orchfs_spdk_device_completion_fn callback,
                     void *context) noexcept {
        std::lock_guard backend_lock(backend_mutex_);
        if (!accepting_.load(std::memory_order_acquire)) {
            return ESHUTDOWN;
        }
        if (backend_ == nullptr) {
            return ENODEV;
        }
        return orchfs_spdk_submit_write(backend_, poller_for(offset), offset,
                                       source, length, callback, context);
    }

    int submit_flush(orchfs_spdk_device_completion_fn callback,
                     void *context) noexcept {
        std::lock_guard backend_lock(backend_mutex_);
        if (!accepting_.load(std::memory_order_acquire)) {
            return ESHUTDOWN;
        }
        if (backend_ == nullptr) {
            return ENODEV;
        }
        return orchfs_spdk_submit_flush(backend_, 0, callback, context);
    }

    int stop() noexcept {
        if (current_poller_service == this) {
            return EDEADLK;
        }

        {
            std::unique_lock stop_lock(stop_mutex_);
            stop_cv_.wait(stop_lock, [this] { return !stop_in_progress_; });
            if (stopped_) {
                return stop_result_;
            }
            stop_in_progress_ = true;
        }

        orchfs_spdk_backend *backend = nullptr;
        {
            std::lock_guard backend_lock(backend_mutex_);
            accepting_.store(false, std::memory_order_release);
            backend = backend_;
            if (backend != nullptr) {
                orchfs_spdk_request_stop(backend);
            }
        }

        int result = first_error_.load(std::memory_order_acquire);
        if (!pollers_joined_ && backend != nullptr) {
            // A constructor or thread-creation failure can leave configured
            // pollers without an owning thread. No qpair for these indices has
            // ever been touched elsewhere, so the stopping thread owns them.
            for (std::size_t index = pollers_.size();
                 index < poller_cpus_.size(); ++index) {
                int stopped = 0;
                while (!stopped) {
                    const int error = orchfs_spdk_poll(
                        backend, index, 0, nullptr, &stopped);
                    record_error(error);
                    if (result == 0 && error != 0) {
                        result = error;
                    }
                }
            }
            bool all_joined = true;
            for (auto &thread : pollers_) {
                if (thread.joinable()) {
                    try {
                        thread.join();
                    } catch (const std::system_error &error) {
                        const int join_error = error.code().value() > 0
                                                   ? error.code().value()
                                                   : EIO;
                        record_error(join_error);
                        if (result == 0) {
                            result = join_error;
                        }
                        all_joined = false;
                    }
                }
            }
            if (all_joined) {
                pollers_.clear();
                pollers_joined_ = true;
            }
        }

        int close_error = 0;
        if (pollers_joined_) {
            std::lock_guard backend_lock(backend_mutex_);
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

        {
            std::lock_guard stop_lock(stop_mutex_);
            stop_result_ = result;
            stopped_ = pollers_joined_ && close_error == 0;
            stop_in_progress_ = false;
        }
        stop_cv_.notify_all();
        return result;
    }

    size_t poller_count() const noexcept { return poller_cpus_.size(); }
    uint32_t lba_size() const noexcept { return lba_size_; }
    uint64_t capacity_bytes() const noexcept { return capacity_bytes_; }
    bool fully_stopped() noexcept {
        std::lock_guard stop_lock(stop_mutex_);
        return stopped_;
    }

  private:
    DeviceService(orchfs_spdk_backend *backend, std::vector<unsigned> cpus)
        : backend_(backend),
          poller_cpus_(std::move(cpus)),
          lba_size_(orchfs_spdk_lba_size(backend)),
          capacity_bytes_(orchfs_spdk_capacity_bytes(backend)) {}

    void record_error(int error) noexcept {
        if (error == 0) {
            return;
        }
        int expected = 0;
        (void)first_error_.compare_exchange_strong(
            expected, error, std::memory_order_acq_rel);
    }

    int start_pollers() noexcept {
        try {
            pollers_.reserve(poller_cpus_.size());
            for (std::size_t index = 0; index < poller_cpus_.size(); ++index) {
                pollers_.emplace_back([this, index] { poll_loop(index); });
            }
        } catch (const std::system_error &error) {
            record_error(error.code().value() > 0 ? error.code().value() : EAGAIN);
        } catch (const std::bad_alloc &) {
            record_error(ENOMEM);
        } catch (...) {
            record_error(EIO);
        }

        if (pollers_.size() != poller_cpus_.size()) {
            accepting_.store(false, std::memory_order_release);
            orchfs_spdk_request_stop(backend_);
            /* Pollers without a thread still need their qpair stop state driven
             * before the controller can be detached. */
            for (std::size_t index = pollers_.size();
                 index < poller_cpus_.size(); ++index) {
                int stopped = 0;
                while (!stopped) {
                    const int error = orchfs_spdk_poll(
                        backend_, index, 0, nullptr, &stopped);
                    record_error(error);
                }
            }
            return first_error_.load(std::memory_order_acquire);
        }

        std::unique_lock lock(start_mutex_);
        start_cv_.wait(lock, [this] {
            return ready_pollers_ == poller_cpus_.size();
        });
        const int error = first_error_.load(std::memory_order_acquire);
        if (error != 0) {
            accepting_.store(false, std::memory_order_release);
            orchfs_spdk_request_stop(backend_);
        }
        return error;
    }

    void publish_ready() noexcept {
        {
            std::lock_guard lock(start_mutex_);
            ++ready_pollers_;
        }
        start_cv_.notify_one();
    }

    void poll_loop(std::size_t index) noexcept {
        struct PollerScope final {
            DeviceService *previous;
            explicit PollerScope(DeviceService *service) noexcept
                : previous(current_poller_service) {
                current_poller_service = service;
            }
            ~PollerScope() { current_poller_service = previous; }
        } poller_scope(this);

        cpu_set_t affinity;
        CPU_ZERO(&affinity);
        CPU_SET(poller_cpus_[index], &affinity);
        const int affinity_error = ::pthread_setaffinity_np(
            ::pthread_self(), sizeof(affinity), &affinity);
        if (affinity_error != 0) {
            record_error(affinity_error);
            accepting_.store(false, std::memory_order_release);
            publish_ready();
            orchfs_spdk_request_stop(backend_);
        } else {
            int stopped = 0;
            const int error =
                orchfs_spdk_poll(backend_, index, 0, nullptr, &stopped);
            record_error(error);
            publish_ready();
            if (error != 0) {
                accepting_.store(false, std::memory_order_release);
                orchfs_spdk_request_stop(backend_);
            }
            if (stopped) {
                return;
            }
        }

        for (;;) {
            size_t completed = 0;
            int stopped = 0;
            const int error = orchfs_spdk_poll(
                backend_, index, 0, &completed, &stopped);
            if (error != 0) {
                record_error(error);
                accepting_.store(false, std::memory_order_release);
                orchfs_spdk_request_stop(backend_);
            }
            if (stopped) {
                return;
            }
            if (completed == 0) {
                std::this_thread::yield();
            }
        }
    }

    std::size_t poller_for(uint64_t offset) const noexcept {
        return static_cast<std::size_t>(
            (offset / kPollerStripeBytes) % poller_cpus_.size());
    }

    orchfs_spdk_backend *backend_{};
    std::mutex backend_mutex_;
    std::vector<unsigned> poller_cpus_;
    std::vector<std::thread> pollers_;
    const uint32_t lba_size_{};
    const uint64_t capacity_bytes_{};
    std::atomic<bool> accepting_{true};
    std::atomic<int> first_error_{0};

    std::mutex start_mutex_;
    std::condition_variable start_cv_;
    std::size_t ready_pollers_{};

    std::mutex stop_mutex_;
    std::condition_variable stop_cv_;
    bool stop_in_progress_{};
    bool pollers_joined_{};
    bool stopped_{};
    int stop_result_{};
};

std::mutex global_mutex;
std::shared_ptr<DeviceService> global_service;

std::shared_ptr<DeviceService> service_snapshot() noexcept {
    std::lock_guard lock(global_mutex);
    return global_service;
}

struct BlockingCompletion {
    std::mutex mutex;
    std::condition_variable cv;
    bool done{};
    int error{};
    size_t bytes{};
};

void complete_blocking(void *context, int error, size_t bytes) noexcept {
    auto &completion = *static_cast<BlockingCompletion *>(context);
    {
        std::lock_guard lock(completion.mutex);
        completion.error = error;
        completion.bytes = bytes;
        completion.done = true;
    }
    completion.cv.notify_one();
}

template <typename Submit>
int wait_for_completion(size_t expected_bytes, Submit &&submit) noexcept {
    if (current_poller_service != nullptr) {
        return EDEADLK;
    }
    BlockingCompletion completion;
    const int submit_error =
        submit(&complete_blocking, static_cast<void *>(&completion));
    if (submit_error != 0) {
        return submit_error;
    }
    std::unique_lock lock(completion.mutex);
    completion.cv.wait(lock, [&completion] { return completion.done; });
    if (completion.error != 0) {
        return completion.error;
    }
    return completion.bytes == expected_bytes ? 0 : EIO;
}

} // namespace

extern "C" {

int orchfs_spdk_device_start(void) {
    std::lock_guard lock(global_mutex);
    if (global_service) {
        return EALREADY;
    }
    std::shared_ptr<DeviceService> service;
    const int error = DeviceService::create(service);
    if (error == 0) {
        global_service = std::move(service);
    }
    return error;
}

int orchfs_spdk_device_stop(void) {
    if (current_poller_service != nullptr) {
        return EDEADLK;
    }
    auto service = service_snapshot();
    if (!service) {
        return 0;
    }
    const int error = service->stop();
    if (service->fully_stopped()) {
        std::lock_guard lock(global_mutex);
        if (global_service == service) {
            global_service.reset();
        }
    }
    return error;
}

int orchfs_spdk_device_is_running(void) {
    return service_snapshot() ? 1 : 0;
}

int orchfs_spdk_device_submit_read(
    uint64_t offset, void *destination, size_t length,
    orchfs_spdk_device_completion_fn callback, void *callback_context) {
    auto service = service_snapshot();
    return service ? service->submit_read(offset, destination, length, callback,
                                          callback_context)
                   : ENODEV;
}

int orchfs_spdk_device_submit_write(
    uint64_t offset, const void *source, size_t length,
    orchfs_spdk_device_completion_fn callback, void *callback_context) {
    auto service = service_snapshot();
    return service ? service->submit_write(offset, source, length, callback,
                                           callback_context)
                   : ENODEV;
}

int orchfs_spdk_device_submit_flush(
    orchfs_spdk_device_completion_fn callback, void *callback_context) {
    auto service = service_snapshot();
    return service ? service->submit_flush(callback, callback_context) : ENODEV;
}

int orchfs_spdk_device_read(uint64_t offset, void *destination, size_t length) {
    return wait_for_completion(length, [&](auto callback, void *context) {
        return orchfs_spdk_device_submit_read(offset, destination, length,
                                              callback, context);
    });
}

int orchfs_spdk_device_write(uint64_t offset, const void *source, size_t length) {
    return wait_for_completion(length, [&](auto callback, void *context) {
        return orchfs_spdk_device_submit_write(offset, source, length, callback,
                                               context);
    });
}

int orchfs_spdk_device_flush(void) {
    return wait_for_completion(0, [&](auto callback, void *context) {
        return orchfs_spdk_device_submit_flush(callback, context);
    });
}

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
