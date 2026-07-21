#include "req_kernel.h"

#include "../KernelFS/kernel_func.h"
#include "../config/protocol.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

static int request_alloc(int64_t function, void* response, int64_t count,
                         int64_t return_type)
{
    if(function < 0 || function > DEALLOC_BLOCK_FUNC)
        return EINVAL;
    return orchfs_kfs_alloc_direct(function, count, return_type, response);
}

static int request_dealloc(int64_t function, const void* blocks,
                           int64_t count, int64_t parameter_type)
{
    if(function < 0 || function > DEALLOC_BLOCK_FUNC)
        return EINVAL;
    return orchfs_kfs_dealloc_direct(
        function, count, parameter_type, (const int64_t*)blocks);
}

void orch_time_stamp(struct timespec* time)
{
    clock_gettime(CLOCK_REALTIME, time);
}

int request_inode_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_INODE_FUNC, output, count, type);
}

int request_idxnd_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_INXND_FUNC, output, count, type);
}

int request_virnd_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_VIRND_FUNC, output, count, type);
}

int request_bufmeta_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_BUFMETA_FUNC, output, count, type);
}

int request_page_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_PAGE_FUNC, output, count, type);
}

int request_block_id_arr(void* output, int64_t count, int64_t type)
{
    return request_alloc(ALLOC_BLOCK_FUNC, output, count, type);
}

int send_dealloc_inode_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_INODE_FUNC, blocks, count, type);
}

int send_dealloc_idxnd_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_INXND_FUNC, blocks, count, type);
}

int send_dealloc_virnd_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_VIRND_FUNC, blocks, count, type);
}

int send_dealloc_bufmeta_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_BUFMETA_FUNC, blocks, count, type);
}

int send_dealloc_page_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_PAGE_FUNC, blocks, count, type);
}

int send_dealloc_block_req(void* blocks, int count, int64_t type)
{
    return request_dealloc(DEALLOC_BLOCK_FUNC, blocks, count, type);
}

int64_t request_log_seg(void)
{
    return orchfs_kfs_alloc_log_direct();
}
