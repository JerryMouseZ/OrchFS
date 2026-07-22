#include "spdk_nvme_bridge.h"

#include "spdk_nvme_backend.hpp"

#include <atomic>
#include <cerrno>
#include <cstddef>
#include <memory>
#include <new>
#include <span>
#include <system_error>
#include <vector>

namespace {

class BridgeCompletionPool;

struct BridgeCompletion {
    orchfs_spdk_completion_fn callback{};
    void *context{};
    BridgeCompletionPool *pool{};
};

class BridgeCompletionPool final {
public:
    explicit BridgeCompletionPool(std::size_t count)
        : storage_(std::make_unique<BridgeCompletion[]>(count)) {
        free_.reserve(count);
        for (std::size_t index = 0; index < count; ++index) {
            free_.push_back(&storage_[index]);
        }
    }

    BridgeCompletion *acquire() noexcept {
        Guard guard(lock_);
        if (free_.empty()) {
            return nullptr;
        }
        BridgeCompletion *completion = free_.back();
        free_.pop_back();
        return completion;
    }

    void release(BridgeCompletion *completion) noexcept {
        if (completion == nullptr) {
            return;
        }
        *completion = {};
        Guard guard(lock_);
        free_.push_back(completion);
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

    std::unique_ptr<BridgeCompletion[]> storage_;
    std::vector<BridgeCompletion *> free_;
    std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

} // namespace

struct orchfs_spdk_backend {
    std::unique_ptr<orchfs::nvme::SpdkNvmeBackend> implementation;
    std::vector<std::unique_ptr<BridgeCompletionPool>> completion_pools;
};

namespace {

int error_to_errno(const std::error_code &error) noexcept {
    if (!error) {
        return 0;
    }
    if (error.category() == std::generic_category() ||
        error.category() == std::system_category()) {
        return error.value() > 0 ? error.value() : EIO;
    }
    if (orchfs::nvme::is_nvme_status_error(error)) {
        return EIO;
    }

    using orchfs::nvme::BackendErrc;
    if (error == orchfs::nvme::make_error_code(BackendErrc::controller_busy)) {
        return EBUSY;
    }
    if (error == orchfs::nvme::make_error_code(BackendErrc::wrong_poller_thread)) {
        return EPERM;
    }
    if (error == orchfs::nvme::make_error_code(BackendErrc::stopping)) {
#ifdef ESHUTDOWN
        return ESHUTDOWN;
#else
        return EBUSY;
#endif
    }
    if (error == orchfs::nvme::make_error_code(BackendErrc::not_stopped)) {
        return EBUSY;
    }
    if (error == orchfs::nvme::make_error_code(BackendErrc::reentrant_poll)) {
        return EDEADLK;
    }
    if (error == orchfs::nvme::make_error_code(BackendErrc::namespace_not_found)) {
        return ENODEV;
    }
    return EIO;
}

void release_bridge_completion(BridgeCompletion *completion) noexcept {
    BridgeCompletionPool *pool = completion->pool;
    if (pool != nullptr) {
        pool->release(completion);
    } else {
        delete completion;
    }
}

void bridge_completion(void *context,
                       std::error_code error,
                       std::size_t bytes) noexcept {
    auto *completion = static_cast<BridgeCompletion *>(context);
    const auto callback = completion->callback;
    void *callback_context = completion->context;
    release_bridge_completion(completion);
    callback(callback_context, error_to_errno(error), bytes);
}

orchfs::nvme::Config make_cpp_config(const orchfs_spdk_config &config) {
    orchfs::nvme::Config result;
    if (config.pci_bdf != nullptr) {
        result.pci_bdf = config.pci_bdf;
    }
    result.namespace_id = config.namespace_id;
    result.poller_count = config.poller_count;
    result.queue_depth = config.queue_depth;
    result.bounce_buffers_per_poller = config.bounce_buffers_per_poller;
    result.max_transfer_size = config.max_transfer_size;
    switch (config.write_durability) {
    case ORCHFS_SPDK_DURABILITY_AUTO:
        result.write_durability = orchfs::nvme::WriteDurability::auto_detect;
        break;
    case ORCHFS_SPDK_DURABILITY_COMPLETION:
        result.write_durability = orchfs::nvme::WriteDurability::completion;
        break;
    case ORCHFS_SPDK_DURABILITY_FUA:
        result.write_durability = orchfs::nvme::WriteDurability::fua;
        break;
    case ORCHFS_SPDK_DURABILITY_FLUSH:
        result.write_durability = orchfs::nvme::WriteDurability::flush;
        break;
    }
    if (config.application_name != nullptr) {
        result.application_name = config.application_name;
    }
    if (config.reactor_mask != nullptr) {
        result.reactor_mask = config.reactor_mask;
    }
    if (config.hugepage_directory != nullptr) {
        result.hugepage_directory = config.hugepage_directory;
    }
    result.shared_memory_id = config.shared_memory_id;
    return result;
}

template <typename Submit>
int submit_with_bridge_callback(orchfs_spdk_backend &backend,
                                std::size_t poller_id,
                                orchfs_spdk_completion_fn callback,
                                void *callback_context,
                                Submit &&submit) {
    if (callback == nullptr || poller_id >= backend.completion_pools.size()) {
        return EINVAL;
    }
    BridgeCompletionPool *pool = backend.completion_pools[poller_id].get();
    BridgeCompletion *completion = pool->acquire();
    if (completion != nullptr) {
        completion->pool = pool;
    } else {
        completion = new (std::nothrow) BridgeCompletion();
    }
    if (completion == nullptr) {
        return ENOMEM;
    }
    completion->callback = callback;
    completion->context = callback_context;

    std::error_code error = submit(&bridge_completion, completion);
    if (error) {
        release_bridge_completion(completion);
        return error_to_errno(error);
    }
    return 0;
}

} // namespace

extern "C" {

void orchfs_spdk_config_init(orchfs_spdk_config *config) {
    if (config == nullptr) {
        return;
    }
    *config = orchfs_spdk_config{
        .pci_bdf = nullptr,
        .namespace_id = 1,
        .poller_count = 1,
        .queue_depth = 32,
        .bounce_buffers_per_poller = 32,
        .max_transfer_size = 1024U * 1024U,
        .write_durability = ORCHFS_SPDK_DURABILITY_AUTO,
        .application_name = "orchfs_kfs",
        .reactor_mask = "0x1",
        .hugepage_directory = nullptr,
        .shared_memory_id = -1,
    };
}

int orchfs_spdk_is_compiled(void) {
    return orchfs::nvme::SpdkNvmeBackend::compiled_with_spdk() ? 1 : 0;
}

int orchfs_spdk_open(const orchfs_spdk_config *config,
                     orchfs_spdk_backend **backend) {
    if (config == nullptr || backend == nullptr) {
        return EINVAL;
    }
    *backend = nullptr;

    try {
        std::error_code error;
        auto implementation =
            orchfs::nvme::SpdkNvmeBackend::open(make_cpp_config(*config), error);
        if (error) {
            return error_to_errno(error);
        }
        if (!implementation) {
            return EIO;
        }

        std::vector<std::unique_ptr<BridgeCompletionPool>> completion_pools;
        try {
            const std::size_t poller_count = implementation->poller_count();
            completion_pools.reserve(poller_count);
            const std::size_t completion_count =
                static_cast<std::size_t>(config->queue_depth) * 2U + 16U;
            for (std::size_t index = 0; index < poller_count; ++index) {
                completion_pools.push_back(
                    std::make_unique<BridgeCompletionPool>(completion_count));
            }
        } catch (const std::bad_alloc &) {
            implementation->request_stop();
            for (std::size_t index = 0; index < implementation->poller_count();
                 ++index) {
                (void)implementation->poll(index);
            }
            (void)implementation->close();
            return ENOMEM;
        }

        auto wrapper = std::unique_ptr<orchfs_spdk_backend>(
            new (std::nothrow) orchfs_spdk_backend{
                .implementation = std::move(implementation),
                .completion_pools = std::move(completion_pools),
            });
        if (!wrapper) {
            implementation->request_stop();
            for (std::size_t index = 0; index < implementation->poller_count(); ++index) {
                (void)implementation->poll(index);
            }
            (void)implementation->close();
            return ENOMEM;
        }
        *backend = wrapper.release();
        return 0;
    } catch (const std::bad_alloc &) {
        return ENOMEM;
    } catch (...) {
        return EIO;
    }
}

int orchfs_spdk_submit_read(orchfs_spdk_backend *backend,
                            size_t poller_id,
                            uint64_t offset,
                            void *destination,
                            size_t length,
                            orchfs_spdk_completion_fn callback,
                            void *callback_context) {
    if (backend == nullptr || backend->implementation == nullptr ||
        (destination == nullptr && length != 0)) {
        return EINVAL;
    }
    return submit_with_bridge_callback(
        *backend,
        poller_id,
        callback,
        callback_context,
        [&](orchfs::nvme::CompletionCallback cpp_callback, void *cpp_context) {
            return backend->implementation->submit_read(
                poller_id,
                offset,
                std::span<std::byte>(static_cast<std::byte *>(destination), length),
                cpp_callback,
                cpp_context);
        });
}

int orchfs_spdk_submit_write(orchfs_spdk_backend *backend,
                             size_t poller_id,
                             uint64_t offset,
                             const void *source,
                             size_t length,
                             orchfs_spdk_completion_fn callback,
                             void *callback_context) {
    if (backend == nullptr || backend->implementation == nullptr ||
        (source == nullptr && length != 0)) {
        return EINVAL;
    }
    return submit_with_bridge_callback(
        *backend,
        poller_id,
        callback,
        callback_context,
        [&](orchfs::nvme::CompletionCallback cpp_callback, void *cpp_context) {
            return backend->implementation->submit_write(
                poller_id,
                offset,
                std::span<const std::byte>(
                    static_cast<const std::byte *>(source), length),
                cpp_callback,
                cpp_context);
        });
}

int orchfs_spdk_submit_flush(orchfs_spdk_backend *backend,
                             size_t poller_id,
                             orchfs_spdk_completion_fn callback,
                             void *callback_context) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return EINVAL;
    }
    return submit_with_bridge_callback(
        *backend,
        poller_id,
        callback,
        callback_context,
        [&](orchfs::nvme::CompletionCallback cpp_callback, void *cpp_context) {
            return backend->implementation->submit_flush(
                poller_id, cpp_callback, cpp_context);
        });
}

int orchfs_spdk_register_memory(orchfs_spdk_backend *backend, void *address,
                                size_t length) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return EINVAL;
    }
    return error_to_errno(
        backend->implementation->register_memory(address, length));
}

int orchfs_spdk_unregister_memory(orchfs_spdk_backend *backend,
                                  void *address, size_t length) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return EINVAL;
    }
    return error_to_errno(
        backend->implementation->unregister_memory(address, length));
}

int orchfs_spdk_poll(orchfs_spdk_backend *backend,
                     size_t poller_id,
                     uint32_t max_completions,
                     size_t *completed_requests,
                     int *stopped) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return EINVAL;
    }
    const orchfs::nvme::PollResult result =
        backend->implementation->poll(poller_id, max_completions);
    if (completed_requests != nullptr) {
        *completed_requests = result.completed_requests;
    }
    if (stopped != nullptr) {
        *stopped = result.stopped ? 1 : 0;
    }
    return error_to_errno(result.error);
}

void orchfs_spdk_request_stop(orchfs_spdk_backend *backend) {
    if (backend != nullptr && backend->implementation != nullptr) {
        backend->implementation->request_stop();
    }
}

int orchfs_spdk_poller_stopped(const orchfs_spdk_backend *backend,
                               size_t poller_id) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return 0;
    }
    return backend->implementation->poller_stopped(poller_id) ? 1 : 0;
}

int orchfs_spdk_close(orchfs_spdk_backend *backend) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return EINVAL;
    }
    const std::error_code error = backend->implementation->close();
    if (error) {
        return error_to_errno(error);
    }
    delete backend;
    return 0;
}

size_t orchfs_spdk_poller_count(const orchfs_spdk_backend *backend) {
    return backend == nullptr || backend->implementation == nullptr
               ? 0
               : backend->implementation->poller_count();
}

uint32_t orchfs_spdk_lba_size(const orchfs_spdk_backend *backend) {
    return backend == nullptr || backend->implementation == nullptr
               ? 0
               : backend->implementation->lba_size();
}

uint32_t orchfs_spdk_max_transfer_size(const orchfs_spdk_backend *backend) {
    return backend == nullptr || backend->implementation == nullptr
               ? 0
               : backend->implementation->max_transfer_size();
}

uint64_t orchfs_spdk_capacity_bytes(const orchfs_spdk_backend *backend) {
    return backend == nullptr || backend->implementation == nullptr
               ? 0
               : backend->implementation->capacity_bytes();
}

orchfs_spdk_write_durability orchfs_spdk_effective_write_durability(
    const orchfs_spdk_backend *backend) {
    if (backend == nullptr || backend->implementation == nullptr) {
        return ORCHFS_SPDK_DURABILITY_AUTO;
    }
    switch (backend->implementation->write_durability()) {
    case orchfs::nvme::WriteDurability::completion:
        return ORCHFS_SPDK_DURABILITY_COMPLETION;
    case orchfs::nvme::WriteDurability::fua:
        return ORCHFS_SPDK_DURABILITY_FUA;
    case orchfs::nvme::WriteDurability::flush:
        return ORCHFS_SPDK_DURABILITY_FLUSH;
    case orchfs::nvme::WriteDurability::auto_detect:
        return ORCHFS_SPDK_DURABILITY_AUTO;
    }
    return ORCHFS_SPDK_DURABILITY_AUTO;
}

int orchfs_spdk_volatile_write_cache_present(
    const orchfs_spdk_backend *backend) {
    return backend != nullptr && backend->implementation != nullptr &&
           backend->implementation->volatile_write_cache_present();
}

} // extern "C"
