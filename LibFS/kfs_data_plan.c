#include "index.h"
#include "libspace.h"
#include "meta_cache.h"
#include "migrate.h"

#include "../KernelFS/device.h"
#include "../config/config.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

int create_strata_structure(root_id_t root_id, int64_t inode,
                            off_info_pt block, int64_t file_block,
                            int64_t start, int64_t length)
{
    const int64_t first_page = start >> ORCH_PAGE_BW;
    const int64_t last_page = (start + length - 1) >> ORCH_PAGE_BW;
    if(block->ndtype == SSD_BLOCK)
        block->ndtype = STRATA_NODE;
    else if(block->ndtype != STRATA_NODE)
        return EINVAL;

    for(int64_t page = first_page; page <= last_page; ++page)
    {
        if(block->nvm_page_id[page] != EMPTY_BLKID)
            continue;
        const int64_t nvm_page = require_nvm_page_id();
        const int64_t metadata = require_buffer_metadata_id();
        if(nvm_page < 0 || metadata < 0)
            return ENOSPC;
        const int insert_error = insert_strata_page_and_metabuf(
            root_id, inode, file_block, page, nvm_page, metadata);
        if(insert_error != 0)
            return insert_error > 0 ? insert_error : EIO;
        block->nvm_page_id[page] = nvmpage_to_devaddr(nvm_page);
        block->buf_meta_id[page] = bufmetaid_to_devaddr(metadata);
        int16_t initial[ORCH_BUFMETA_SIZE / sizeof(int16_t)];
        memset(initial, 0, sizeof(initial));
        nvm_write(initial, sizeof(initial), block->buf_meta_id[page]);
    }
    return 0;
}

static int append_blocks(root_id_t root_id, int64_t inode,
                         int64_t first, int64_t last,
                         int64_t last_byte)
{
    int64_t ssd_count = 0;
    int64_t page_count = 0;
    const int64_t end_position = last_byte & (ORCH_BLOCK_SIZE - 1);
    if(end_position + 1 != ORCH_BLOCK_SIZE)
    {
        ssd_count = last - first;
        page_count = end_position / ORCH_PAGE_SIZE + 1;
    }
    else
        ssd_count = last - first + 1;

    if(ssd_count > 0)
    {
        ssd_blk_id_t* blocks = malloc((size_t)ssd_count * sizeof(*blocks));
        if(blocks == NULL)
            return ENOMEM;
        const int allocation_error =
            require_ssd_block_ids(ssd_count, blocks);
        if(allocation_error != 0)
        {
            free(blocks);
            return allocation_error;
        }
        const int append_error = append_ssd_blocks(
            root_id, inode, ssd_count, blocks);
        if(append_error != 0)
        {
            free(blocks);
            return append_error > 0 ? append_error : EIO;
        }
        free(blocks);
    }
    if(page_count > 0)
    {
        nvm_page_id_t* pages = malloc((size_t)page_count * sizeof(*pages));
        if(pages == NULL)
            return ENOMEM;
        for(int64_t index = 0; index < page_count; ++index)
        {
            pages[index] = require_nvm_page_id();
            if(pages[index] < 0)
            {
                free(pages);
                return ENOSPC;
            }
        }
        const int append_error = append_nvm_pages(
            root_id, inode, page_count, pages);
        if(append_error != 0)
        {
            free(pages);
            return append_error > 0 ? append_error : EIO;
        }
        free(pages);
    }
    return 0;
}

int acquire_op_blk_info(root_id_t root_id, int64_t inode,
                        off_info_pt blocks, int64_t first,
                        int64_t last, int64_t last_byte)
{
    for(int64_t file_block = first; file_block <= last; ++file_block)
    {
        const int64_t slot = file_block - first;
        blocks[slot] = query_offset_info(root_id, inode, file_block);
        if(blocks[slot].ndtype == EMPTY_BLK_TYPE)
        {
            const int append_error = append_blocks(
                root_id, inode, file_block, last, last_byte);
            if(append_error != 0)
                return append_error;
            for(int64_t appended = file_block; appended <= last; ++appended)
            {
                const int64_t appended_slot = appended - first;
                blocks[appended_slot] =
                    query_offset_info(root_id, inode, appended);
                if(blocks[appended_slot].ndtype == EMPTY_BLK_TYPE)
                    return EIO;
            }
            break;
        }
        if(blocks[slot].ndtype != VIR_LEAF_NODE)
            continue;

        const int required_pages = file_block == last
            ? (int)(last_byte / ORCH_PAGE_SIZE + 1)
            : VLN_SLOT_SUM;
        int added = 0;
        for(int page = 0; page < required_pages; ++page)
        {
            if(blocks[slot].nvm_page_id[page] == EMPTY_BLKID)
            {
                const int64_t page_id = require_nvm_page_id();
                if(page_id < 0)
                    return ENOSPC;
                if(append_single_nvm_page(root_id, inode, page_id) != 0)
                    return EIO;
                ++added;
            }
        }
        if(added != 0)
            blocks[slot] = query_offset_info(root_id, inode, file_block);
        if(required_pages == VLN_SLOT_SUM)
            add_migrate_node(orchfs_migration_state(), blocks + slot,
                             inode, added);
    }
    return 0;
}
