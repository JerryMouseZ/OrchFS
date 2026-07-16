#ifndef ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H
#define ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H

#ifdef __cplusplus
extern "C" {
#endif

/* KFS process-wide coroutine runtime and IPC server. Return errno values. */
int orchfs_async_server_start(void);
void orchfs_async_server_request_stop(void);
int orchfs_async_server_stop(void);
int orchfs_async_server_is_running(void);

#ifdef __cplusplus
}
#endif

#endif /* ORCHFS_KERNELFS_ASYNC_SERVER_BRIDGE_H */
