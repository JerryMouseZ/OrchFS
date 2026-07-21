#ifndef ORCHFS_LIB_LOG_H
#define ORCHFS_LIB_LOG_H

#include <stddef.h>
#include <stdint.h>

struct orchfs_log_transaction;

int init_mem_log(void);
void free_mem_log(void);
void write_create_log(int64_t block, int type);
void write_delete_log(int64_t block, int type);
void write_change_log(int64_t block, int type, void* data,
                      int32_t offset, int32_t length);

int orchfs_log_transaction_create(struct orchfs_log_transaction** output);
void orchfs_log_transaction_bind(struct orchfs_log_transaction* transaction);
void orchfs_log_transaction_unbind(struct orchfs_log_transaction* transaction);
int orchfs_log_transaction_commit(struct orchfs_log_transaction* transaction);
int orchfs_log_transaction_is_empty(
    const struct orchfs_log_transaction* transaction);
int orchfs_log_transaction_commit_group(
    struct orchfs_log_transaction** transactions, size_t count, int* errors);
void orchfs_log_transaction_abort(struct orchfs_log_transaction* transaction);
int orchfs_log_transaction_record_allocation(
    struct orchfs_log_transaction* transaction, int type, int64_t block);
int orchfs_log_record_current_allocation(int type, int64_t block);
int orchfs_log_defer_current_release(int type, int64_t block);
int orchfs_log_transaction_add_pmem(
    struct orchfs_log_transaction* transaction, int64_t offset,
    const void* data, int32_t length);

#endif
