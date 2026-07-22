#include "kfs_core_api.h"

#include "index.h"
#include "lib_inode.h"
#include "lib_log.h"
#include "libspace.h"
#include "meta_cache.h"
#include "migrate.h"

#include "../KernelFS/device.h"
#include "../KernelFS/balloc.h"
#include "../KernelFS/type.h"
#include "../config/config.h"
#include "../config/log_config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string.h>

static int core_initialized;
static int64_t core_root_inode = -1;

static int reap_persistent_orphans(void)
{
    int64_t inode_id = -1;
    while((inode_id = orchfs_bitmap_next_allocated(
               INODE_BMP, inode_id)) >= 0)
    {
        if(inode_id == core_root_inode)
            continue;
        orch_inode_pt inode = inodeid_to_memaddr(inode_id);
        if(inode == NULL)
            return errno != 0 ? errno : EIO;
        if(inode->i_number != inode_id ||
           (inode->reserved[0] & ORCHFS_INODE_FLAG_ORPHAN) == 0)
            continue;
        struct orchfs_log_transaction* transaction = NULL;
        int error = orchfs_log_transaction_create(&transaction);
        if(error != 0)
            return error;
        orchfs_log_transaction_bind(transaction);
        error = delete_inode(inode_id);
        orchfs_log_transaction_unbind(transaction);
        if(error != 0)
        {
            orchfs_log_transaction_abort(transaction);
            return error;
        }
        error = orchfs_log_transaction_commit(transaction);
        if(error != 0)
            return error;
    }
    return 0;
}

int orchfs_core_validate_format(void)
{
    return orchfs_log_validate_format();
}

int orchfs_core_recover(void)
{
    return orchfs_log_recover();
}

int orchfs_core_initialize(void)
{
    if(core_initialized)
        return EALREADY;
    init_all_ext();
    const int cache_error = init_metadata_cache();
    if(cache_error != 0)
        return cache_error;
    const int log_error = init_mem_log();
    if(log_error != 0)
    {
        close_metadata_cache();
        return log_error;
    }

    unsigned char superblock_bytes[ORCH_SUPER_BLK_SIZE];
    nvm_read(superblock_bytes, sizeof(superblock_bytes), OFFSET_SUPER_BLK);
    orch_super_blk_t superblock;
    memcpy(&superblock, superblock_bytes, sizeof(superblock));
    if(superblock.magic_num[0] != ORCH_MAGIC_NUM ||
       superblock.magic_num[1] != ORCH_MAGIC_NUM ||
       superblock.magic_num[2] != ORCH_MAGIC_NUM)
    {
        free_mem_log();
        close_metadata_cache();
        return EINVAL;
    }
    core_root_inode = (int64_t)superblock.root_inode;
    const int migration_error = orchfs_migration_initialize();
    if(migration_error != 0)
    {
        free_mem_log();
        close_metadata_cache();
        core_root_inode = -1;
        return migration_error;
    }
    core_initialized = 1;
    const int orphan_error = reap_persistent_orphans();
    if(orphan_error != 0)
    {
        core_initialized = 0;
        orchfs_migration_shutdown();
        free_mem_log();
        close_metadata_cache();
        core_root_inode = -1;
        return orphan_error;
    }
    return 0;
}

void orchfs_core_shutdown(void)
{
    if(!core_initialized)
        return;
    orchfs_migration_shutdown();
    close_metadata_cache();
    free_mem_log();
    return_all_ext();
    core_root_inode = -1;
    core_initialized = 0;
}

int64_t orchfs_core_root_inode(void)
{
    return core_initialized ? core_root_inode : -1;
}

/* Data-planning helpers are intentionally narrower than the legacy fd API. */
int acquire_op_blk_info(root_id_t root_id, ino_id_t ino_id,
                        off_info_pt block_info, int64_t start_block,
                        int64_t end_block, int64_t last_block_byte);
int create_strata_structure(root_id_t root_id, int64_t inode,
                            off_info_pt block_info, int64_t file_block,
                            int64_t start, int64_t length);

static int copy_inode(int64_t inode_id, struct orchfs_core_inode* result)
{
    if(result == NULL || inode_id < 0 || inode_id >= MAX_INODE_NUM)
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    result->inode = inode->i_number;
    result->size = inode->i_size;
    result->index_root = inode->i_idxroot;
    result->link_count = inode->i_nlink;
    result->uid = inode->i_uid;
    result->gid = inode->i_gid;
    result->mode = inode->i_mode;
    result->type = inode->i_type;
    result->atime_seconds = inode->i_atim.tv_sec;
    result->atime_nanoseconds = inode->i_atim.tv_nsec;
    result->mtime_seconds = inode->i_mtim.tv_sec;
    result->mtime_nanoseconds = inode->i_mtim.tv_nsec;
    result->ctime_seconds = inode->i_ctim.tv_sec;
    result->ctime_nanoseconds = inode->i_ctim.tv_nsec;
    return 0;
}

int orchfs_core_create_inode(int32_t type, uint32_t mode,
                             struct orchfs_core_inode* result)
{
    if(!core_initialized || result == NULL ||
       (type != SIMPLE_FILE && type != DIR_FILE))
        return EINVAL;
    const int64_t inode_id = inode_create(type);
    if(inode_id < 0)
        return errno != 0 ? errno : ENOSPC;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    inode->i_number = inode_id;
    inode->i_mode = (int32_t)mode |
        (type == DIR_FILE ? S_IFDIR : S_IFREG);
    write_change_log(inode_id, INODE_OP, inode, 0, ORCH_INODE_SIZE);
    return copy_inode(inode_id, result);
}

int orchfs_core_delete_inode(int64_t inode)
{
    if(!core_initialized || inode < 0 || inode == core_root_inode)
        return EINVAL;
    return delete_inode(inode);
}

int orchfs_core_set_orphan(int64_t inode_id, int orphaned)
{
    if(!core_initialized || inode_id < 0 ||
       (orphaned != 0 && orphaned != 1))
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    if(orphaned)
        inode->reserved[0] |= ORCHFS_INODE_FLAG_ORPHAN;
    else
        inode->reserved[0] &= (uint8_t)~ORCHFS_INODE_FLAG_ORPHAN;
    write_change_log(inode_id, INODE_OP, inode, 0, ORCH_INODE_SIZE);
    return 0;
}

int orchfs_core_snapshot(int64_t inode, struct orchfs_core_inode* result)
{
    return copy_inode(inode, result);
}

static void copy_block(off_info_t source, struct orchfs_core_block* result)
{
    result->type = source.ndtype;
    result->ssd_device_offset = source.ssd_dev_addr;
    result->file_block = source.offset_ans;
    for(int index = 0; index < ORCHFS_CORE_PAGE_COUNT; ++index)
    {
        result->nvm_page_offset[index] = source.nvm_page_id[index];
        result->buffer_metadata_offset[index] = source.buf_meta_id[index];
    }
}

int orchfs_core_query_block(int64_t inode_id, int64_t file_block,
                            struct orchfs_core_block* result)
{
    if(result == NULL || inode_id < 0 || file_block < 0)
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    off_info_t block = query_offset_info(inode->i_idxroot, inode_id,
                                         file_block);
    if(block.ndtype == EMPTY_BLK_TYPE)
        return ENODATA;
    copy_block(block, result);
    return 0;
}

int orchfs_core_prepare_write_blocks(int64_t inode_id, int64_t first_block,
                                     int64_t last_block,
                                     int64_t last_block_byte,
                                     struct orchfs_core_block* result)
{
    if(result == NULL || inode_id < 0 || first_block < 0 ||
       last_block < first_block || last_block_byte < 0 ||
       last_block_byte >= ORCH_BLOCK_SIZE)
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    const int64_t count = last_block - first_block + 1;
    off_info_pt blocks = malloc((size_t)count * sizeof(*blocks));
    if(blocks == NULL)
        return ENOMEM;
    const int prepare_error = acquire_op_blk_info(
        inode->i_idxroot, inode_id, blocks, first_block, last_block,
        last_block_byte);
    if(prepare_error != 0)
    {
        free(blocks);
        return prepare_error;
    }
    for(int64_t index = 0; index < count; ++index)
    {
        if(blocks[index].ndtype == EMPTY_BLK_TYPE)
        {
            free(blocks);
            return EIO;
        }
        copy_block(blocks[index], result + index);
    }
    free(blocks);
    return 0;
}

int orchfs_core_ensure_strata(int64_t inode_id, int64_t file_block,
                              int64_t start, int64_t length,
                              struct orchfs_core_block* result)
{
    if(result == NULL || inode_id < 0 || file_block < 0 || start < 0 ||
       length <= 0 || start >= ORCH_BLOCK_SIZE ||
       length > ORCH_BLOCK_SIZE - start)
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    off_info_t block = query_offset_info(inode->i_idxroot, inode_id,
                                         file_block);
    if(block.ndtype != SSD_BLOCK && block.ndtype != STRATA_NODE)
        return EINVAL;
    const int strata_error = create_strata_structure(
        inode->i_idxroot, inode_id, &block, file_block, start, length);
    if(strata_error != 0)
        return strata_error;
    block = query_offset_info(inode->i_idxroot, inode_id, file_block);
    if(block.ndtype != STRATA_NODE)
        return EIO;
    copy_block(block, result);
    return 0;
}

static int valid_pmem_range(int64_t offset, size_t length)
{
    return offset >= 0 && (uint64_t)offset <= (uint64_t)PMEM_LEN &&
           length <= (uint64_t)PMEM_LEN - (uint64_t)offset;
}

int orchfs_core_read_pmem(int64_t offset, void* destination, size_t length)
{
    if((destination == NULL && length != 0) ||
       !valid_pmem_range(offset, length))
        return EINVAL;
    if(length != 0)
        nvm_read(destination, (int64_t)length, offset);
    return 0;
}

int orchfs_core_write_pmem(int64_t offset, const void* source, size_t length)
{
    if((source == NULL && length != 0) || !valid_pmem_range(offset, length))
        return EINVAL;
    if(length != 0)
        nvm_write((void*)source, (int64_t)length, offset);
    return 0;
}

int orchfs_core_persist_pmem(void)
{
    return orchfs_log_sync();
}

int orchfs_core_set_size(int64_t inode_id, uint64_t size)
{
    if(inode_id < 0 || size > INT64_MAX ||
       size > (UINT64_C(1) << (KEY_LEN + ORCH_BLOCK_BW)))
        return EFBIG;
    orch_inode_pt inode = inodeid_to_memaddr(inode_id);
    if(inode == NULL || inode->i_number != inode_id)
        return ENOENT;
    if(inode->i_type != SIMPLE_FILE && inode->i_type != DIR_FILE)
        return EIO;
    inode_change_file_size(inode, (int64_t)size);
    return 0;
}

int orchfs_core_sync_inode(int64_t inode)
{
    if(inode < 0)
        return EINVAL;
    sync_inode_and_index(inode);
    return orchfs_log_sync();
}
