#ifndef ORCHFS_KFS_CORE_API_H
#define ORCHFS_KFS_CORE_API_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

enum {
    ORCHFS_CORE_UNKNOWN = 0,
    ORCHFS_CORE_DIRECTORY = 1,
    ORCHFS_CORE_REGULAR = 2,
    ORCHFS_CORE_EMPTY_BLOCK = -1,
    ORCHFS_CORE_VIRTUAL_BLOCK = 3,
    ORCHFS_CORE_STRATA_BLOCK = 4,
    ORCHFS_CORE_SSD_BLOCK = 5,
    ORCHFS_CORE_PAGE_COUNT = 8,
    ORCHFS_CORE_DIRENT_SIZE = 256,
    ORCHFS_CORE_DIRENT_NAME_MAX = 230,
};

struct orchfs_core_inode {
    int64_t inode;
    int64_t size;
    int64_t index_root;
    int64_t link_count;
    int32_t uid;
    int32_t gid;
    int32_t mode;
    int32_t type;
    int64_t atime_seconds;
    int64_t atime_nanoseconds;
    int64_t mtime_seconds;
    int64_t mtime_nanoseconds;
    int64_t ctime_seconds;
    int64_t ctime_nanoseconds;
};

struct orchfs_core_block {
    int64_t type;
    int64_t ssd_device_offset;
    int64_t nvm_page_offset[ORCHFS_CORE_PAGE_COUNT];
    int64_t buffer_metadata_offset[ORCHFS_CORE_PAGE_COUNT];
    int64_t file_block;
};

struct orchfs_core_dirent {
    int64_t inode;
    int64_t offset;
    uint16_t name_length;
    uint8_t type;
    char name[ORCHFS_CORE_DIRENT_NAME_MAX + 1];
};

int orchfs_core_initialize(void);
int orchfs_core_validate_format(void);
int orchfs_core_recover(void);
void orchfs_core_shutdown(void);
int64_t orchfs_core_root_inode(void);
int orchfs_core_create_inode(int32_t type, uint32_t mode,
                             struct orchfs_core_inode* result);
int orchfs_core_delete_inode(int64_t inode);
int orchfs_core_set_orphan(int64_t inode, int orphaned);

int orchfs_core_snapshot(int64_t inode, struct orchfs_core_inode* result);

int orchfs_core_query_block(int64_t inode, int64_t file_block,
                            struct orchfs_core_block* result);
int orchfs_core_prepare_write_blocks(int64_t inode, int64_t first_block,
                                     int64_t last_block,
                                     int64_t last_block_byte,
                                     struct orchfs_core_block* result);
int orchfs_core_ensure_strata(int64_t inode, int64_t file_block,
                              int64_t start, int64_t length,
                              struct orchfs_core_block* result);

int orchfs_core_read_pmem(int64_t offset, void* destination, size_t length);
int orchfs_core_write_pmem(int64_t offset, const void* source, size_t length);
/* Order and persist PMEM stores issued by the calling inode-owner worker. */
int orchfs_core_persist_pmem(void);
int orchfs_core_set_size(int64_t inode, uint64_t size);
int orchfs_core_sync_inode(int64_t inode);

#ifdef __cplusplus
}
#endif

#endif
