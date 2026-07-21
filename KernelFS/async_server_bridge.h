#ifndef ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H
#define ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* KFS process-wide coroutine runtime and IPC server. Return errno values.
 * The Runtime starts before the device so SPDK qpairs can register worker
 * poll hooks, and stops only after the device has drained and detached. */
int orchfs_async_runtime_start(void);
void *orchfs_async_runtime_handle(void);
size_t orchfs_async_runtime_worker_count(void);
size_t orchfs_async_current_worker(void);
int orchfs_async_runtime_stop(void);
int orchfs_async_server_start(void);
void orchfs_async_server_request_stop(void);
int orchfs_async_server_stop(void);
int orchfs_async_server_is_running(void);
int orchfs_async_migration_start(void);
void orchfs_async_schedule_migration(void);
void orchfs_async_migration_stop(void);

/* Lock-free MPSC candidate queue used only by the in-process coroutine core.
 * Legacy LibFS builds retain their original LRU implementation. */
int orchfs_async_migration_enqueue_candidate(
    int64_t inode, int64_t block_offset, const int64_t *nvm_page_addresses,
    size_t page_count);
int orchfs_async_migration_take_candidate(
    int64_t *inode, int64_t *block_offset, int64_t *nvm_page_addresses,
    size_t page_count);
int orchfs_async_migration_candidates_pending(void);

#ifdef __cplusplus
}
#endif

#endif /* ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H */
