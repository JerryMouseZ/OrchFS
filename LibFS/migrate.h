#ifndef ORCHFS_MIGRATE_H
#define ORCHFS_MIGRATE_H

#include <stddef.h>
#include <stdint.h>

#define DEFAULT_MIGRATE_NUM 1024
#define MIGRATE_PERCENTAGE 10
#define CAN_USE_PERCENTAGE 10
#define DO_MIGRATE 1
#define SLEEP 0

struct LRU_node_info_t
{
    int64_t ino_id;
    int64_t offset;
    int64_t nvm_page_addr[10];
};
typedef struct LRU_node_info_t LRU_node_info_t;

struct migrate_info_t
{
    int64_t migrate_num;
    int64_t nvm_page_used;
    int64_t all_nvm_page;
    int64_t can_use_page_num;
    int64_t mig_threshold;
    int64_t mig_state;
    int64_t all_mig_blk;
    int64_t max_page_use;
};
typedef struct migrate_info_t migrate_info_t;
typedef migrate_info_t* migrate_info_pt;

struct orchfs_migration_plan;
struct offset_info_t;

int orchfs_migration_initialize(void);
void orchfs_migration_shutdown(void);
migrate_info_pt orchfs_migration_state(void);

void add_migrate_node(migrate_info_pt ignored,
                      struct offset_info_t* information,
                      int64_t inode, int64_t new_pages);

int orchfs_prepare_migration(struct orchfs_migration_plan** plan);
int64_t orchfs_migration_inode(const struct orchfs_migration_plan* plan);
uint64_t orchfs_migration_file_offset(
    const struct orchfs_migration_plan* plan);
int orchfs_prepare_migration_io(struct orchfs_migration_plan* plan);
const void* orchfs_migration_data(const struct orchfs_migration_plan* plan);
uint64_t orchfs_migration_length(const struct orchfs_migration_plan* plan);
uint64_t orchfs_migration_device_offset(
    const struct orchfs_migration_plan* plan);
int64_t orchfs_migration_new_ssd_block(
    const struct orchfs_migration_plan* plan);
int orchfs_finish_migration(struct orchfs_migration_plan* plan,
                            int io_error);
int orchfs_migration_has_pending(void);

#endif
