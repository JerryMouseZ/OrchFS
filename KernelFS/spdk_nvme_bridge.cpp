#include "spdk_nvme_bridge.h"

#include "spdk_nvme_backend.hpp"

#include <cerrno>
#include <cstddef>
#include <memory>
#include <new>
#include <span>
#include <system_error>

struct orchfs_spdk_backend {
    std::unique_ptr<orchfs::nvme::SpdkNvmeBackend> implementation;
};

namespace {

struct BridgeCompletion {
    orchfs_spdk_completion_fn callback;
    void *context;
};

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

void bridge_completion(void *context,
                       std::error_code error,
                       std::size_t bytes) noexcept {
    std::unique_ptr<BridgeCompletion> completion(
        static_cast<BridgeCompletion *>(context));
    completion->callback(completion->context, error_to_errno(error), bytes);
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
int submit_with_bridge_callback(orchfs_spdk_completion_fn callback,
                                void *callback_context,
                                Submit &&submit) {
    if (callback == nullptr) {
        return EINVAL;
    }
    auto completion =
        std::unique_ptr<BridgeCompletion>(new (std::nothrow) BridgeCompletion{
            .callback = callback,
            .context = callback_context,
        });
    if (!completion) {
        return ENOMEM;
    }

    std::error_code error = submit(&bridge_completion, completion.get());
    if (error) {
        return error_to_errno(error);
    }
    (void)completion.release();
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
        .queue_depth = 128,
        .bounce_buffers_per_poller = 64,
        .max_transfer_size = 0,
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

        auto wrapper = std::unique_ptr<orchfs_spdk_backend>(
            new (std::nothrow) orchfs_spdk_backend{
                .implementation = std::move(implementation),
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
        callback,
        callback_context,
        [&](orchfs::nvme::CompletionCallback cpp_callback, void *cpp_context) {
            return backend->implementation->submit_flush(
                poller_id, cpp_callback, cpp_context);
        });
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

} // extern "C"
