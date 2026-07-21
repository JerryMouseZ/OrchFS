/* manage libfs metadata */
#ifndef LIBSPACE_H
#define LIBSPACE_H

#include <stdint.h>

// block type
#define MEM_ADDRESS      0
#define SSD_ADDRESS      1
#define NVM_ADDRESS      2

#define MIN_EXT_ID                   1
#define INODE_EXT                    1
#define IDXND_EXT                    2
#define VIRND_EXT                    3
#define BUFMETA_EXT                  4
#define PAGE_EXT                     5
#define BLOCK_EXT                    6
#define MAX_EXT_ID                   6

// return type
#define RET_BLK_ID           0
#define RET_BLK_ADDR         1

// parameter type
#define PAR_BLK_ID           0
#define PAR_BLK_ADDR         1

int init_all_ext(void);


int return_all_ext(void);




int64_t require_inode_id(void);


int64_t require_index_node_id(void);


int64_t require_virindex_node_id(void);


int64_t require_buffer_metadata_id(void);


int64_t require_nvm_page_id(void);


int64_t require_ssd_block_id(void);

int require_ssd_block_ids(int64_t count, int64_t* block_ids);




void release_inode(int64_t inode_id);


void release_index_node(int64_t idx_id);


void release_virindex_node(int64_t virnd_id);


void release_buffer_metadata(int64_t buf_id);


void release_nvm_page(int64_t page_id);


void release_ssd_block(int64_t block_id);


#endif
