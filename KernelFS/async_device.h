#ifndef ORCHFS_KERNELFS_ASYNC_DEVICE_H
#define ORCHFS_KERNELFS_ASYNC_DEVICE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*orchfs_device_completion_fn)(void* context, int error_number,
                                             size_t bytes);

int submit_read_data_from_devs(void* dst, int64_t len, int64_t offset,
                               orchfs_device_completion_fn completion,
                               void* context);
int submit_write_data_to_devs(const void* src, int64_t len, int64_t offset,
                              orchfs_device_completion_fn completion,
                              void* context);
int submit_device_sync(orchfs_device_completion_fn completion, void* context);
int orchfs_device_register_dma_region(void* address, size_t length);
int orchfs_device_unregister_dma_region(void* address, size_t length);

#ifdef __cplusplus
}
#endif

#endif
