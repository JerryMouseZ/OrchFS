#include "migrate.h"

#include "index.h"
#include "lib_inode.h"
#include "libspace.h"
#include "meta_cache.h"

#include "../KernelFS/async_server_bridge.h"
#include "../KernelFS/device.h"
#include "../config/config.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct orchfs_migration_plan
{
    LRU_node_info_t node;
    root_id_t root_id;
    int64_t new_ssd_block_id;
    uint64_t device_offset;
    void* block_data;
    int prepared;
    int requeue_on_error;
};

static migrate_info_t migration;
static int migration_initialized;

int orchfs_migration_initialize(void)
{
    if(migration_initialized)
        return EALREADY;
    memset(&migration, 0, sizeof(migration));
    migration.migrate_num = DEFAULT_MIGRATE_NUM;
    migration.all_nvm_page = MAX_PAGE_NUM;
    migration.can_use_page_num =
        migration.all_nvm_page / 100 * CAN_USE_PERCENTAGE;
    migration.mig_threshold =
        migration.can_use_page_num / 100 * MIGRATE_PERCENTAGE;
    migration.mig_state = SLEEP;
    const int error = orchfs_async_migration_start();
    if(error != 0)
        return error;
    migration_initialized = 1;
    return 0;
}

void orchfs_migration_shutdown(void)
{
    if(!migration_initialized)
        return;
    orchfs_async_migration_stop();
    fprintf(stderr, "all mig num: %lld\n",
            (long long)migration.all_mig_blk);
    fprintf(stderr, "max page num: %lld\n",
            (long long)migration.max_page_use);
    migration_initialized = 0;
}

migrate_info_pt orchfs_migration_state(void)
{
    return migration_initialized ? &migration : NULL;
}

static void requeue(const LRU_node_info_t* node)
{
    (void)orchfs_async_migration_enqueue_candidate(
        node->ino_id, node->offset, node->nvm_page_addr,
        (size_t)(ORCH_BLOCK_SIZE / ORCH_PAGE_SIZE));
}

int orchfs_prepare_migration(struct orchfs_migration_plan** output)
{
    if(output == NULL || !migration_initialized)
        return EINVAL;
    *output = NULL;
    struct orchfs_migration_plan* plan = calloc(1, sizeof(*plan));
    if(plan == NULL)
        return ENOMEM;
    plan->new_ssd_block_id = -1;
    plan->requeue_on_error = 1;
    const int error = orchfs_async_migration_take_candidate(
        &plan->node.ino_id, &plan->node.offset,
        plan->node.nvm_page_addr,
        (size_t)(ORCH_BLOCK_SIZE / ORCH_PAGE_SIZE));
    if(error != 0)
    {
        free(plan);
        return error;
    }
    *output = plan;
    return 0;
}

int64_t orchfs_migration_inode(const struct orchfs_migration_plan* plan)
{
    return plan != NULL ? plan->node.ino_id : -1;
}

uint64_t orchfs_migration_file_offset(
    const struct orchfs_migration_plan* plan)
{
    return plan != NULL && plan->node.offset >= 0
        ? (uint64_t)plan->node.offset << ORCH_BLOCK_BW : 0;
}

int orchfs_prepare_migration_io(struct orchfs_migration_plan* plan)
{
    if(plan == NULL || plan->prepared)
        return EINVAL;
    orch_inode_pt inode = inodeid_to_memaddr(plan->node.ino_id);
    if(inode == NULL || inode->i_number != plan->node.ino_id)
        return ESTALE;
    plan->root_id = inode->i_idxroot;
    off_info_t current = query_offset_info(
        plan->root_id, plan->node.ino_id, plan->node.offset);
    if(current.ndtype != VIR_LEAF_NODE)
    {
        plan->requeue_on_error = 0;
        return ESTALE;
    }
    for(int i = 0; i < VLN_SLOT_SUM; ++i)
    {
        if(current.nvm_page_id[i] != plan->node.nvm_page_addr[i])
        {
            plan->requeue_on_error = 0;
            return ESTALE;
        }
    }

    plan->block_data = malloc(ORCH_BLOCK_SIZE);
    if(plan->block_data == NULL)
        return ENOMEM;
    for(int i = 0; i < VLN_SLOT_SUM; ++i)
        nvm_read((char*)plan->block_data + ORCH_PAGE_SIZE * i,
                 ORCH_PAGE_SIZE, plan->node.nvm_page_addr[i]);
    plan->new_ssd_block_id = require_ssd_block_id();
    if(plan->new_ssd_block_id < 0)
    {
        free(plan->block_data);
        plan->block_data = NULL;
        return ENOSPC;
    }
    plan->device_offset =
        (uint64_t)ssdblk_to_devaddr(plan->new_ssd_block_id);
    plan->prepared = 1;
    return 0;
}

const void* orchfs_migration_data(const struct orchfs_migration_plan* plan)
{
    return plan != NULL ? plan->block_data : NULL;
}

uint64_t orchfs_migration_length(const struct orchfs_migration_plan* plan)
{
    return plan != NULL ? ORCH_BLOCK_SIZE : 0;
}

uint64_t orchfs_migration_device_offset(
    const struct orchfs_migration_plan* plan)
{
    return plan != NULL ? plan->device_offset : 0;
}

int64_t orchfs_migration_new_ssd_block(
    const struct orchfs_migration_plan* plan)
{
    return plan != NULL ? plan->new_ssd_block_id : -1;
}

int orchfs_finish_migration(struct orchfs_migration_plan* plan, int error)
{
    if(plan == NULL)
        return EINVAL;
    if(error == 0 && plan->prepared)
    {
        error = change_virnd_to_ssdblk(
            plan->root_id, plan->node.ino_id, plan->node.offset,
            plan->new_ssd_block_id);
        if(error == 0)
        {
            __atomic_fetch_sub(&migration.nvm_page_used, VLN_SLOT_SUM,
                               __ATOMIC_ACQ_REL);
            __atomic_fetch_add(&migration.all_mig_blk, 1,
                               __ATOMIC_ACQ_REL);
        }
    }
    if(error != 0)
    {
        if(plan->new_ssd_block_id >= 0)
            release_ssd_block(plan->new_ssd_block_id);
        if(plan->requeue_on_error)
            requeue(&plan->node);
    }
    free(plan->block_data);
    free(plan);
    return error;
}

int orchfs_migration_has_pending(void)
{
    return orchfs_async_migration_candidates_pending();
}

void add_migrate_node(migrate_info_pt ignored, struct offset_info_t* info,
                      int64_t inode, int64_t new_pages)
{
    (void)ignored;
    if(!migration_initialized || info == NULL)
        return;
    LRU_node_info_t node;
    memset(&node, 0, sizeof(node));
    node.ino_id = inode;
    node.offset = info->offset_ans;
    for(int i = 0; i < VLN_SLOT_SUM; ++i)
        node.nvm_page_addr[i] = info->nvm_page_id[i];
    const int error = orchfs_async_migration_enqueue_candidate(
        inode, info->offset_ans, node.nvm_page_addr, VLN_SLOT_SUM);
    if(error != 0)
    {
        errno = error;
        perror("enqueue migration candidate");
        return;
    }
    const int64_t used = __atomic_add_fetch(
        &migration.nvm_page_used, new_pages, __ATOMIC_ACQ_REL);
    int64_t maximum = __atomic_load_n(&migration.max_page_use,
                                      __ATOMIC_ACQUIRE);
    while(used > maximum &&
          !__atomic_compare_exchange_n(&migration.max_page_use, &maximum,
                                       used, 1, __ATOMIC_ACQ_REL,
                                       __ATOMIC_ACQUIRE))
        ;
    if(used > migration.mig_threshold)
        orchfs_async_schedule_migration();
}
