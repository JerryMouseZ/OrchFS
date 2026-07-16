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
 * Defaults target 0000:b2:00.0, namespace 1, and up to four CPUs from the
 * process affinity mask. Every qpair is initialized, submitted, polled and
 * freed by its permanent poller thread. These functions return errno values
 * (zero on success).
 *
 * Completion callbacks execute on the selected poller thread. They may submit
 * more asynchronous work, but must not call a blocking compatibility entry
 * point or orchfs_spdk_device_stop(); those calls return EDEADLK. A flush
 * submitted here waits for every logical write accepted before the flush call,
 * across all pollers, before issuing the namespace NVMe FLUSH command.
 */
int orchfs_spdk_device_start(void);
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

/* Blocking compatibility entry points for the existing filesystem core. */
int orchfs_spdk_device_read(uint64_t offset, void *destination, size_t length);
int orchfs_spdk_device_write(uint64_t offset, const void *source, size_t length);
int orchfs_spdk_device_flush(void);

size_t orchfs_spdk_device_poller_count(void);
uint32_t orchfs_spdk_device_lba_size(void);
uint64_t orchfs_spdk_device_capacity_bytes(void);

#ifdef __cplusplus
}
#endif

#endif /* ORCHFS_KERNELFS_SPDK_DEVICE_SERVICE_H */
