#include "log.h"
#include "device.h"

#include "../config/config.h"
#include "../config/log_config.h"

#include "../util/hashtable.h"
#include "../util/orch_list.h"
#include "../util/orch_array.h"

#include <errno.h>

#ifdef __cplusplus       
extern "C"{
#endif

void digest_log(int64_t seg_id)
{
    (void)seg_id;
}

void init_kernel_log()
{
    log_seg_num = (OFFSET_INODE-OFFSET_LOG) / LOG_SEGMENT_SIZE;

    kmem_log_meta = malloc(log_seg_num * sizeof(klog_meta_t));
    for(int64_t i = 0; i < log_seg_num; i++)
    {
        kmem_log_meta[i].used_flag = 0;
        kmem_log_meta[i].klog_data_sp = NULL;
    }
    for(int i = 0; i < MAX_OPEN_FILE; i++)
        file_log_index[i] = create_array(sizeof(int64_t));
    inoid_to_flidx = init_hashtable(DEFAULT);
}

int64_t alloc_log_segment_from_dev()
{
    for(int64_t i = 0; i < log_seg_num; i++)
    {
        if(kmem_log_meta[i].used_flag == 0)
        {
            kmem_log_meta[i].used_flag = 1;
            kmem_log_meta[i].submit_end_off = -1;
            kmem_log_meta[i].sync_len = 0;
            kmem_log_meta[i].last_submit_time = 0;
            kmem_log_meta[i].not_writed_len = LOG_SEGMENT_SIZE-LOG_META_SIZE;

            int64_t dev_off = OFFSET_LOG + LOG_SEGMENT_SIZE * i;
            nvm_write(kmem_log_meta + i, 5*sizeof(int64_t), dev_off);
            kmem_log_meta[i].klog_data_sp = malloc(LOG_SEGMENT_SIZE);

            return i;
        }
    }
    errno = ENOSPC;
    return -1;
}

void sync_file_log(int64_t inode_id)
{
    (void)inode_id;
}

void submit_part_log_segment(int64_t seg_id, int64_t start_addr, int64_t submit_len)
{
    (void)seg_id;
    (void)start_addr;
    (void)submit_len;
}

void submit_log_segment(int64_t seg_id)
{
    (void)seg_id;
}

void close_kernel_log()
{
    for(int64_t i = 0; i < log_seg_num; i++)
    {
        if(kmem_log_meta[i].used_flag == 1)
        {
            digest_log(i);
        }
        if(kmem_log_meta[i].klog_data_sp != NULL)
            free(kmem_log_meta[i].klog_data_sp);
    }
    free(kmem_log_meta);
    for(int i = 0; i < MAX_OPEN_FILE; i++)
        free_array(file_log_index[i]);
    free_hashtable(inoid_to_flidx);
}

#ifdef __cplusplus
}
#endif
