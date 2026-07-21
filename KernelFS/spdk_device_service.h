#ifndef ORCHFS_KERNELFS_SPDK_DEVICE_SERVICE_H
#define ORCHFS_KERNELFS_SPDK_DEVICE_SERVICE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*orchfs_spdk_device_completion_fn)(void *context,
                                                  int error_number,
                                                  size_t bytes);

/*
 * Process-wide SPDK service owned by KFS. Configuration is read once from:
 *   ORCHFS_SPDK_PCI_BDF, ORCHFS_SPDK_NSID, ORCHFS_SPDK_POLLER_COUNT,
 *   ORCHFS_SPDK_QUEUE_DEPTH, ORCHFS_SPDK_BOUNCE_BUFFERS,
 *   ORCHFS_SPDK_MAX_TRANSFER_SIZE, ORCHFS_SPDK_REACTOR_MASK,
 *   ORCHFS_SPDK_HUGEPAGE_DIR, ORCHFS_SPDK_SHM_ID and
 *   ORCHFS_SPDK_CPU_LIST.
 *
 * Standalone compatibility calls poll their qpairs on the calling thread and
 * create no service threads. Runtime-integrated KFS owns exactly one qpair per
 * Runtime worker: the same worker submits, polls, completes and frees it.
 * These functions return errno values (zero on success).
 *
 * Completion callbacks execute on the qpair owner. They may submit
 * more asynchronous work, but must not call a blocking compatibility entry
 * point or orchfs_spdk_device_stop(); those calls return EDEADLK. A flush
 * submitted here waits for every logical write accepted before the flush call,
 * across all pollers, before issuing the namespace NVMe FLUSH command.
 */
int orchfs_spdk_device_start(void);
/* Runtime-integrated mode. runtime_handle must point to the process Runtime;
 * one qpair is polled by each Runtime worker and no poller threads are made. */
int orchfs_spdk_device_start_on_runtime(void *runtime_handle);
int orchfs_spdk_device_stop(void);
int orchfs_spdk_device_is_running(void);

int orchfs_spdk_device_submit_read(
    uint64_t offset, void *destination, size_t length,
    orchfs_spdk_device_completion_fn callback, void *callback_context);
int orchfs_spdk_device_submit_write(
    uint64_t offset, const void *source, size_t length,
    orchfs_spdk_device_completion_fn callback, void *callback_context);
int orchfs_spdk_device_submit_flush(
    orchfs_spdk_device_completion_fn callback, void *callback_context);
int orchfs_spdk_device_register_memory(void *address, size_t length);
int orchfs_spdk_device_unregister_memory(void *address, size_t length);

/* Blocking compatibility entry points for the existing filesystem core. */
#ifdef ORCHFS_FORMATTER
/* Offline formatter only: completion is driven by the calling thread. */
int orchfs_spdk_formatter_read(uint64_t offset, void *destination,
                              size_t length);
int orchfs_spdk_formatter_write(uint64_t offset, const void *source,
                               size_t length);
int orchfs_spdk_formatter_flush(void);
#endif

size_t orchfs_spdk_device_poller_count(void);
uint32_t orchfs_spdk_device_lba_size(void);
uint64_t orchfs_spdk_device_capacity_bytes(void);

#ifdef __cplusplus
}
#endif

#endif /* ORCHFS_KERNELFS_SPDK_DEVICE_SERVICE_H */
