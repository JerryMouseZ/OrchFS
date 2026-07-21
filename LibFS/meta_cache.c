#include "meta_cache.h"

#include "index.h"
#include "../config/config.h"
#include "../KernelFS/device.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

static int64_t cache_base(int cache_id)
{
    if(cache_id == INODE_CACHE)
        return OFFSET_INODE;
    if(cache_id == IDXND_CACHE)
        return OFFSET_INDEX;
    return OFFSET_VIRND;
}

static char* load_segment(int cache_id, int64_t segment)
{
    cache_data_pt cache = cache_data + cache_id;
    if(segment < 0 || segment >= cache->seg_num)
    {
        errno = EINVAL;
        return NULL;
    }
    char* current = atomic_load_explicit(
        cache->meta_memsp_pt + segment, memory_order_acquire);
    if(current != NULL)
        return current;

    const int64_t length = cache->unit_size * cache->seg_blknum;
    char* loaded = malloc((size_t)length);
    if(loaded == NULL)
    {
        errno = ENOMEM;
        return NULL;
    }
    nvm_read(loaded, length, cache_base(cache_id) + segment * length);
    char* expected = NULL;
    if(!atomic_compare_exchange_strong_explicit(
           cache->meta_memsp_pt + segment, &expected, loaded,
           memory_order_release, memory_order_acquire))
    {
        free(loaded);
        loaded = expected;
    }
    return loaded;
}

static int initialize_cache(int cache_id, int64_t unit_size,
                            int64_t segment_blocks, int64_t block_count)
{
    cache_data_pt cache = cache_data + cache_id;
    cache->seg_blknum = segment_blocks;
    cache->unit_size = unit_size;
    cache->seg_num = block_count / segment_blocks;
    cache->meta_memsp_pt = calloc(
        (size_t)cache->seg_num, sizeof(*cache->meta_memsp_pt));
    if(cache->meta_memsp_pt == NULL)
    {
        cache->seg_num = 0;
        return ENOMEM;
    }
    for(int64_t segment = 0; segment < cache->seg_num; ++segment)
        atomic_init(cache->meta_memsp_pt + segment, NULL);
    return 0;
}

int init_metadata_cache(void)
{
    int error = initialize_cache(INODE_CACHE, ORCH_INODE_SIZE,
                                 INODE_CACHE_EXTBLKS, MAX_INODE_NUM);
    if(error == 0)
        error = initialize_cache(IDXND_CACHE, ORCH_IDX_SIZE,
                                 IDXND_CACHE_EXTBLKS, MAX_INDEX_NUM);
    if(error == 0)
        error = initialize_cache(VIRND_CACHE, ORCH_VIRND_SIZE,
                                 VIRND_CACHE_EXTBLKS, MAX_VIRND_NUM);
    if(error != 0)
        close_metadata_cache();
    return error;
}

static void sync_unit(int cache_id, int64_t block)
{
    cache_data_pt cache = cache_data + cache_id;
    const int64_t segment = block / cache->seg_blknum;
    const int64_t position = block % cache->seg_blknum;
    char* memory = load_segment(cache_id, segment);
    if(memory == NULL)
        return;
    nvm_write(memory + position * cache->unit_size, cache->unit_size,
              cache_base(cache_id) + block * cache->unit_size);
}

void sync_inode(int64_t inode_id)
{
    sync_unit(INODE_CACHE, inode_id);
}

void sync_index_blk(int64_t index_id)
{
    sync_unit(IDXND_CACHE, index_id);
}

void sync_virnd_blk(int64_t virtual_node_id)
{
    sync_unit(VIRND_CACHE, virtual_node_id);
}

void close_metadata_cache(void)
{
    for(int cache_id = MIN_CACHE_ID; cache_id <= MAX_CACHE_ID; ++cache_id)
    {
        cache_data_pt cache = cache_data + cache_id;
        const int64_t length = cache->unit_size * cache->seg_blknum;
        for(int64_t segment = 0; segment < cache->seg_num; ++segment)
        {
            char* memory = atomic_load_explicit(
                cache->meta_memsp_pt + segment, memory_order_acquire);
            if(memory == NULL)
                continue;
            nvm_write(memory, length,
                      cache_base(cache_id) + segment * length);
            free(memory);
        }
        free(cache->meta_memsp_pt);
        cache->meta_memsp_pt = NULL;
        cache->seg_num = 0;
    }
}

void create_file_metadata_cache(int64_t inode_id)
{
    (void)load_segment(INODE_CACHE, inode_id / INODE_CACHE_EXTBLKS);
}

void delete_file_metadata_cache(int64_t inode_id)
{
    (void)inode_id;
}

void close_file_metadata_cache(int64_t inode_id)
{
    (void)inode_id;
}

void* inodeid_to_memaddr(int64_t inode_id)
{
    if(inode_id < 0 || inode_id >= MAX_INODE_NUM)
    {
        errno = EINVAL;
        return NULL;
    }
    const int64_t segment = inode_id / INODE_CACHE_EXTBLKS;
    const int64_t position = inode_id % INODE_CACHE_EXTBLKS;
    char* memory = load_segment(INODE_CACHE, segment);
    return memory == NULL ? NULL : memory + position * ORCH_INODE_SIZE;
}

void* indexid_to_memaddr(int64_t inode_id, int64_t index_id, int create_flag)
{
    (void)inode_id;
    (void)create_flag;
    if(index_id < 0 || index_id >= MAX_INDEX_NUM)
    {
        errno = EINVAL;
        return NULL;
    }
    const int64_t segment = index_id / IDXND_CACHE_EXTBLKS;
    const int64_t position = index_id % IDXND_CACHE_EXTBLKS;
    char* memory = load_segment(IDXND_CACHE, segment);
    return memory == NULL ? NULL : memory + position * ORCH_IDX_SIZE;
}

void* virnodeid_to_memaddr(int64_t inode_id, int64_t virtual_node_id,
                           int create_flag)
{
    (void)inode_id;
    (void)create_flag;
    if(virtual_node_id < 0 || virtual_node_id >= MAX_VIRND_NUM)
    {
        errno = EINVAL;
        return NULL;
    }
    const int64_t segment = virtual_node_id / VIRND_CACHE_EXTBLKS;
    const int64_t position = virtual_node_id % VIRND_CACHE_EXTBLKS;
    char* memory = load_segment(VIRND_CACHE, segment);
    return memory == NULL ? NULL : memory + position * ORCH_VIRND_SIZE;
}

int64_t bufmetaid_to_devaddr(int64_t buffer_id)
{
    if(buffer_id < 0 || buffer_id >= MAX_BUFMETA_NUM)
        return -1;
    return buffer_id * ORCH_BUFMETA_SIZE + OFFSET_BUFMETA;
}

int64_t nvmpage_to_devaddr(int64_t page_id)
{
    if(page_id < 0 || page_id >= MAX_PAGE_NUM)
        return -1;
    return page_id * ORCH_PAGE_SIZE + OFFSET_PAGE;
}

int64_t ssdblk_to_devaddr(int64_t block_id)
{
    if(block_id < 0 || block_id >= MAX_BLOCK_NUM)
        return -1;
    return block_id * ORCH_BLOCK_SIZE + OFFSET_BLOCK;
}
