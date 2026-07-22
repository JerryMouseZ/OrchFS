#ifndef ORCHFS_KERNELFS_SPDK_NVME_BRIDGE_H
#define ORCHFS_KERNELFS_SPDK_NVME_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct orchfs_spdk_backend orchfs_spdk_backend;

typedef enum orchfs_spdk_write_durability {
    ORCHFS_SPDK_DURABILITY_AUTO = 0,
    ORCHFS_SPDK_DURABILITY_COMPLETION = 1,
    ORCHFS_SPDK_DURABILITY_FUA = 2,
    ORCHFS_SPDK_DURABILITY_FLUSH = 3,
} orchfs_spdk_write_durability;

typedef struct orchfs_spdk_config {
    const char *pci_bdf;
    uint32_t namespace_id;
    size_t poller_count;
    uint32_t queue_depth;
    size_t bounce_buffers_per_poller;
    uint32_t max_transfer_size;
    orchfs_spdk_write_durability write_durability;
    const char *application_name;
    const char *reactor_mask;
    const char *hugepage_directory;
    int shared_memory_id;
} orchfs_spdk_config;

typedef void (*orchfs_spdk_completion_fn)(void *context,
                                          int error_number,
                                          size_t bytes);

void orchfs_spdk_config_init(orchfs_spdk_config *config);
int orchfs_spdk_is_compiled(void);

// Returns errno-style status.  ENOTSUP is returned by builds without SPDK.
int orchfs_spdk_open(const orchfs_spdk_config *config,
                     orchfs_spdk_backend **backend);

int orchfs_spdk_submit_read(orchfs_spdk_backend *backend,
                            size_t poller_id,
                            uint64_t offset,
                            void *destination,
                            size_t length,
                            orchfs_spdk_completion_fn callback,
                            void *callback_context);
int orchfs_spdk_submit_write(orchfs_spdk_backend *backend,
                             size_t poller_id,
                             uint64_t offset,
                             const void *source,
                             size_t length,
                             orchfs_spdk_completion_fn callback,
                             void *callback_context);
int orchfs_spdk_submit_flush(orchfs_spdk_backend *backend,
                             size_t poller_id,
                             orchfs_spdk_completion_fn callback,
                             void *callback_context);
int orchfs_spdk_register_memory(orchfs_spdk_backend *backend, void *address,
                                size_t length);
int orchfs_spdk_unregister_memory(orchfs_spdk_backend *backend, void *address,
                                  size_t length);

// completed_requests and stopped may be NULL.  Callbacks run synchronously from
// this function on the poller thread. Recursive poll of the same poller is
// rejected with EDEADLK.
int orchfs_spdk_poll(orchfs_spdk_backend *backend,
                     size_t poller_id,
                     uint32_t max_completions,
                     size_t *completed_requests,
                     int *stopped);

void orchfs_spdk_request_stop(orchfs_spdk_backend *backend);
int orchfs_spdk_poller_stopped(const orchfs_spdk_backend *backend,
                               size_t poller_id);

// Fails with EBUSY until request_stop() has been issued and every poller has
// reported stopped.  On success the handle is destroyed.
int orchfs_spdk_close(orchfs_spdk_backend *backend);

size_t orchfs_spdk_poller_count(const orchfs_spdk_backend *backend);
uint32_t orchfs_spdk_lba_size(const orchfs_spdk_backend *backend);
uint32_t orchfs_spdk_max_transfer_size(const orchfs_spdk_backend *backend);
uint64_t orchfs_spdk_capacity_bytes(const orchfs_spdk_backend *backend);
orchfs_spdk_write_durability orchfs_spdk_effective_write_durability(
    const orchfs_spdk_backend *backend);
int orchfs_spdk_volatile_write_cache_present(
    const orchfs_spdk_backend *backend);

#ifdef __cplusplus
}
#endif

#endif // ORCHFS_KERNELFS_SPDK_NVME_BRIDGE_H
