#ifndef REQ_KERNEL_H
#define REQ_KERNEL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>


/* Request a batch of inode from the kernel*/
int request_inode_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);

/* Request a batch of index node from the kernel*/
int request_idxnd_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);

/* Request a batch of virtual index node from the kernel*/
int request_virnd_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);

/* Request a batch of buffer metadata from the kernel*/
int request_bufmeta_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);

/* Request a batch of nvm page from the kernel*/
int request_page_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);

/* Request a batch of ssd block from the kernel*/
int request_block_id_arr(void* ret_info_buf, int64_t req_blk_num, int64_t ret_type);


/* Request to release a batch of inode from the kernel*/
int send_dealloc_inode_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);

/* Request to release a batch of index node from the kernel*/
int send_dealloc_idxnd_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);

/* Request to release a batch of virtual index node from the kernel*/
int send_dealloc_virnd_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);

/* Request to release a batch of buffer metadata from the kernel*/
int send_dealloc_bufmeta_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);

/* Request to release a batch of nvm page from the kernel */
int send_dealloc_page_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);

/* Request to release a batch of ssd block from the kernel */
int send_dealloc_block_req(void* send_buf, int dealloc_blk_num, int64_t ret_type);


int64_t request_log_seg(void);


void orch_time_stamp(struct timespec* time);

#endif
