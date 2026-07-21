#include "libspace.h"

#include "lib_log.h"
#include "req_kernel.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Allocation is performed directly against the Runtime-worker-sharded KFS
 * bitmap.  The former process-wide extent caches needed spin ownership and
 * could reserve thousands of objects that never became part of a transaction.
 * A single-object reservation is cheap in-process and is immediately attached
 * to the current journal transaction for rollback. */
static int request_one(int type, int64_t* block)
{
    int64_t response[3] = {0};
    int error;
    switch(type)
    {
        case INODE_EXT:
            error = request_inode_id_arr(response, 1, RET_BLK_ID);
            break;
        case IDXND_EXT:
            error = request_idxnd_id_arr(response, 1, RET_BLK_ID);
            break;
        case VIRND_EXT:
            error = request_virnd_id_arr(response, 1, RET_BLK_ID);
            break;
        case BUFMETA_EXT:
            error = request_bufmeta_id_arr(response, 1, RET_BLK_ID);
            break;
        case PAGE_EXT:
            error = request_page_id_arr(response, 1, RET_BLK_ID);
            break;
        case BLOCK_EXT:
            error = request_block_id_arr(response, 1, RET_BLK_ID);
            break;
        default:
            return EINVAL;
    }
    if(error != 0)
        return error;
    if(response[1] != 1 || response[2] < 0)
        return EIO;
    *block = response[2];
    return 0;
}

static int release_one(int type, int64_t block)
{
    switch(type)
    {
        case INODE_EXT:
            return send_dealloc_inode_req(&block, 1, PAR_BLK_ID);
        case IDXND_EXT:
            return send_dealloc_idxnd_req(&block, 1, PAR_BLK_ID);
        case VIRND_EXT:
            return send_dealloc_virnd_req(&block, 1, PAR_BLK_ID);
        case BUFMETA_EXT:
            return send_dealloc_bufmeta_req(&block, 1, PAR_BLK_ID);
        case PAGE_EXT:
            return send_dealloc_page_req(&block, 1, PAR_BLK_ID);
        case BLOCK_EXT:
            return send_dealloc_block_req(&block, 1, PAR_BLK_ID);
        default:
            return EINVAL;
    }
}

static int64_t reserve(int type)
{
    int64_t block = -1;
    int error = request_one(type, &block);
    if(error != 0)
    {
        errno = error;
        return -1;
    }
    error = orchfs_log_record_current_allocation(type, block);
    if(error != 0)
    {
        (void)release_one(type, block);
        errno = error;
        return -1;
    }
    return block;
}

static void release(int type, int64_t block)
{
    if(block < 0)
    {
        errno = EINVAL;
        return;
    }
    const int deferred = orchfs_log_defer_current_release(type, block);
    if(deferred > 0)
        return;
    if(deferred < 0)
    {
        errno = -deferred;
        return;
    }
    const int error = release_one(type, block);
    if(error != 0)
        errno = error;
}

int init_all_ext(void)
{
    return 0;
}

int return_all_ext(void)
{
    return 0;
}

int64_t require_inode_id(void) { return reserve(INODE_EXT); }
int64_t require_index_node_id(void) { return reserve(IDXND_EXT); }
int64_t require_virindex_node_id(void) { return reserve(VIRND_EXT); }
int64_t require_buffer_metadata_id(void) { return reserve(BUFMETA_EXT); }
int64_t require_nvm_page_id(void) { return reserve(PAGE_EXT); }
int64_t require_ssd_block_id(void) { return reserve(BLOCK_EXT); }

int require_ssd_block_ids(int64_t count, int64_t* blocks)
{
    if(count < 0 || (count != 0 && blocks == NULL))
        return EINVAL;
    if(count == 0)
        return 0;
    if(count > INT_MAX ||
       (uint64_t)count > SIZE_MAX / sizeof(int64_t) - 2)
        return EOVERFLOW;

    int64_t* response = malloc(((size_t)count + 2) * sizeof(*response));
    if(response == NULL)
        return ENOMEM;
    int error = request_block_id_arr(response, count, RET_BLK_ID);
    if(error != 0)
    {
        free(response);
        return error;
    }
    if(response[1] != count)
    {
        (void)send_dealloc_block_req(response + 2, (int)count, PAR_BLK_ID);
        free(response);
        return EIO;
    }

    int64_t recorded = 0;
    for(; recorded < count; ++recorded)
    {
        const int64_t block = response[recorded + 2];
        if(block < 0)
        {
            error = EIO;
            break;
        }
        error = orchfs_log_record_current_allocation(BLOCK_EXT, block);
        if(error != 0)
            break;
        blocks[recorded] = block;
    }
    if(error != 0)
    {
        /* Successfully journaled entries belong to the transaction and are
         * released by abort.  Entries not recorded must be returned here. */
        (void)send_dealloc_block_req(response + 2 + recorded,
                                     (int)(count - recorded), PAR_BLK_ID);
    }
    free(response);
    return error;
}

void release_inode(int64_t block) { release(INODE_EXT, block); }
void release_index_node(int64_t block) { release(IDXND_EXT, block); }
void release_virindex_node(int64_t block) { release(VIRND_EXT, block); }
void release_buffer_metadata(int64_t block) { release(BUFMETA_EXT, block); }
void release_nvm_page(int64_t block) { release(PAGE_EXT, block); }
void release_ssd_block(int64_t block) { release(BLOCK_EXT, block); }

#ifdef __cplusplus
}
#endif
