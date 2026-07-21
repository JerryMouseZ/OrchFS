#ifndef KERNEL_FUNC_H
#define KERNEL_FUNC_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

void mkfs();


void init_kernelFS();
void init_kernelFS_direct();


void close_kernelFS();


int orchfs_kfs_alloc_direct(int64_t func_type, int64_t alloc_blk_num,
                            int64_t return_type, void* ret_info_buf);
int orchfs_kfs_dealloc_direct(int64_t func_type, int64_t dealloc_blk_num,
                              int64_t parameter_type,
                              const int64_t* block_ids);
int64_t orchfs_kfs_alloc_log_direct(void);

#ifdef __cplusplus
}
#endif

#endif
