#include "lib_log.h"

#include "libspace.h"
#include "meta_cache.h"
#include "req_kernel.h"
#include "../config/config.h"
#include "../config/log_config.h"
#include "../KernelFS/device.h"

#include <errno.h>
#include <immintrin.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum transaction_record_kind
{
    RECORD_CREATE,
    RECORD_DELETE,
    RECORD_CHANGE,
    RECORD_PMEM,
};

struct transaction_record
{
    enum transaction_record_kind kind;
    int64_t block;
    int type;
    int32_t offset;
    int32_t length;
    unsigned char* data;
    struct transaction_record* next;
};

struct allocation_record
{
    int type;
    int64_t block;
    struct allocation_record* next;
};

struct orchfs_log_transaction
{
    struct transaction_record* first;
    struct transaction_record* last;
    struct allocation_record* allocations;
    struct allocation_record* deallocations;
    int error;
};

struct journal_state
{
    int64_t base;
    int64_t cursor;
    int64_t end;
};

static struct journal_state journal;
static _Thread_local struct orchfs_log_transaction* current_transaction;

static void free_records(struct orchfs_log_transaction* transaction)
{
    struct transaction_record* record = transaction->first;
    while(record != NULL)
    {
        struct transaction_record* next = record->next;
        free(record->data);
        free(record);
        record = next;
    }
    transaction->first = NULL;
    transaction->last = NULL;
}

static void free_allocations(struct orchfs_log_transaction* transaction,
                             int release)
{
    struct allocation_record* allocation = transaction->allocations;
    while(allocation != NULL)
    {
        struct allocation_record* next = allocation->next;
        if(release)
        {
            if(allocation->type == INODE_EXT)
                release_inode(allocation->block);
            else if(allocation->type == IDXND_EXT)
                release_index_node(allocation->block);
            else if(allocation->type == VIRND_EXT)
                release_virindex_node(allocation->block);
            else if(allocation->type == BUFMETA_EXT)
                release_buffer_metadata(allocation->block);
            else if(allocation->type == PAGE_EXT)
                release_nvm_page(allocation->block);
            else if(allocation->type == BLOCK_EXT)
                release_ssd_block(allocation->block);
        }
        free(allocation);
        allocation = next;
    }
    transaction->allocations = NULL;
}

static void free_deallocations(struct orchfs_log_transaction* transaction,
                               int apply)
{
    struct allocation_record* allocation = transaction->deallocations;
    while(allocation != NULL)
    {
        struct allocation_record* next = allocation->next;
        if(apply)
        {
            if(allocation->type == INODE_EXT)
                release_inode(allocation->block);
            else if(allocation->type == IDXND_EXT)
                release_index_node(allocation->block);
            else if(allocation->type == VIRND_EXT)
                release_virindex_node(allocation->block);
            else if(allocation->type == BUFMETA_EXT)
                release_buffer_metadata(allocation->block);
            else if(allocation->type == PAGE_EXT)
                release_nvm_page(allocation->block);
            else if(allocation->type == BLOCK_EXT)
                release_ssd_block(allocation->block);
        }
        free(allocation);
        allocation = next;
    }
    transaction->deallocations = NULL;
}

static int metadata_location(int type, int64_t block, int32_t offset,
                             int32_t length, void** memory,
                             int64_t* device_offset)
{
    if(block < 0 || offset < 0 || length < 0 || memory == NULL ||
       device_offset == NULL)
        return EINVAL;
    int64_t base;
    int64_t unit;
    void* block_memory;
    if(type == INODE_OP)
    {
        base = OFFSET_INODE;
        unit = ORCH_INODE_SIZE;
        block_memory = inodeid_to_memaddr(block);
    }
    else if(type == IDXND_OP)
    {
        base = OFFSET_INDEX;
        unit = ORCH_IDX_SIZE;
        block_memory = indexid_to_memaddr(0, block, CREATE);
    }
    else if(type == VIRND_OP)
    {
        base = OFFSET_VIRND;
        unit = ORCH_VIRND_SIZE;
        block_memory = virnodeid_to_memaddr(0, block, CREATE);
    }
    else
        return EINVAL;
    if(block_memory == NULL || offset > unit || length > unit - offset)
        return EINVAL;
    *memory = (unsigned char*)block_memory + offset;
    *device_offset = base + block * unit + offset;
    return 0;
}

static int append_raw_record(const struct transaction_record* record)
{
    size_t data_length = record->kind == RECORD_CHANGE ||
                         record->kind == RECORD_PMEM
        ? (size_t)record->length : 0;
    size_t record_length = record->kind == RECORD_CHANGE ||
                           record->kind == RECORD_PMEM
        ? 3 * sizeof(int64_t) + data_length : 2 * sizeof(int64_t);
    if(record_length > (size_t)(journal.end - journal.base - LOG_META_SIZE))
        return E2BIG;
    if(journal.cursor + (int64_t)record_length > journal.end)
        journal.cursor = journal.base + LOG_META_SIZE;

    unsigned char* bytes = malloc(record_length);
    if(bytes == NULL)
        return ENOMEM;
    int operation = record->kind == RECORD_CREATE ? CREATE_OP
        : record->kind == RECORD_DELETE ? DELETE_OP : CHANGE_OP;
    int record_type = record->kind == RECORD_PMEM ? BUFMETA_OP : record->type;
    int64_t* header = (int64_t*)bytes;
    header[0] = ((int64_t)(record_type | operation)) << 32;
    header[1] = record->block;
    if(record->kind == RECORD_CHANGE || record->kind == RECORD_PMEM)
    {
        header[0] += record->length;
        header[2] = ((int64_t)record->offset << 32) + record->length;
        memcpy(header + 3, record->data, data_length);
    }
    nvm_write(bytes, (int64_t)record_length, journal.cursor);
    journal.cursor += (int64_t)record_length;
    free(bytes);
    return 0;
}

int init_mem_log(void)
{
    const int64_t segment = request_log_seg();
    if(segment < 0)
    {
        journal.base = -1;
        journal.cursor = -1;
        journal.end = -1;
        return ENOSPC;
    }
    journal.base = OFFSET_LOG + segment * LOG_SEGMENT_SIZE;
    journal.cursor = journal.base + LOG_META_SIZE;
    journal.end = journal.base + LOG_SEGMENT_SIZE;
    return 0;
}

void free_mem_log(void)
{
    _mm_sfence();
}

int orchfs_log_transaction_create(struct orchfs_log_transaction** output)
{
    if(output == NULL)
        return EINVAL;
    if(journal.base < 0)
        return ENOSPC;
    *output = calloc(1, sizeof(**output));
    return *output == NULL ? ENOMEM : 0;
}

void orchfs_log_transaction_bind(struct orchfs_log_transaction* transaction)
{
    if(transaction == NULL || current_transaction != NULL)
        abort();
    current_transaction = transaction;
}

void orchfs_log_transaction_unbind(struct orchfs_log_transaction* transaction)
{
    if(transaction == NULL || current_transaction != transaction)
        abort();
    current_transaction = NULL;
}

int orchfs_log_transaction_record_allocation(
    struct orchfs_log_transaction* transaction, int type, int64_t block)
{
    if(transaction == NULL || type < MIN_EXT_ID || type > MAX_EXT_ID ||
       block < 0)
        return EINVAL;
    struct allocation_record* allocation = malloc(sizeof(*allocation));
    if(allocation == NULL)
    {
        transaction->error = ENOMEM;
        return ENOMEM;
    }
    allocation->type = type;
    allocation->block = block;
    allocation->next = transaction->allocations;
    transaction->allocations = allocation;
    return 0;
}

int orchfs_log_record_current_allocation(int type, int64_t block)
{
    if(current_transaction == NULL)
        return 0;
    return orchfs_log_transaction_record_allocation(
        current_transaction, type, block);
}

int orchfs_log_defer_current_release(int type, int64_t block)
{
    if(current_transaction == NULL)
        return 0;
    if(type < MIN_EXT_ID || type > MAX_EXT_ID || block < 0)
    {
        current_transaction->error = EINVAL;
        return -EINVAL;
    }
    struct allocation_record* allocation = malloc(sizeof(*allocation));
    if(allocation == NULL)
    {
        current_transaction->error = ENOMEM;
        return -ENOMEM;
    }
    allocation->type = type;
    allocation->block = block;
    allocation->next = current_transaction->deallocations;
    current_transaction->deallocations = allocation;
    return 1;
}

static void add_record(enum transaction_record_kind kind, int64_t block,
                       int type, const void* data, int32_t offset,
                       int32_t length)
{
    if(current_transaction == NULL)
        abort();
    if(current_transaction->error != 0)
        return;
    if(type < MIN_BLKTYPE_OP || type > MAX_BLKTYPE_OP || block < 0 ||
       offset < 0 || length < 0 || (length != 0 && data == NULL))
    {
        current_transaction->error = EINVAL;
        return;
    }
    struct transaction_record* record = calloc(1, sizeof(*record));
    if(record == NULL)
    {
        current_transaction->error = ENOMEM;
        return;
    }
    if(length != 0)
    {
        record->data = malloc((size_t)length);
        if(record->data == NULL)
        {
            free(record);
            current_transaction->error = ENOMEM;
            return;
        }
        memcpy(record->data, (const unsigned char*)data + offset,
               (size_t)length);
    }
    record->kind = kind;
    record->block = block;
    record->type = type;
    record->offset = offset;
    record->length = length;
    if(current_transaction->last == NULL)
        current_transaction->first = record;
    else
        current_transaction->last->next = record;
    current_transaction->last = record;
}

void write_create_log(int64_t block, int type)
{
    add_record(RECORD_CREATE, block, type, NULL, 0, 0);
}

void write_delete_log(int64_t block, int type)
{
    add_record(RECORD_DELETE, block, type, NULL, 0, 0);
}

void write_change_log(int64_t block, int type, void* data,
                      int32_t offset, int32_t length)
{
    add_record(RECORD_CHANGE, block, type, data, offset, length);
}

int orchfs_log_transaction_add_pmem(
    struct orchfs_log_transaction* transaction, int64_t offset,
    const void* data, int32_t length)
{
    if(transaction == NULL || current_transaction != NULL || offset < 0 ||
       length < 0 || (length != 0 && data == NULL))
        return EINVAL;
    struct orchfs_log_transaction* previous = current_transaction;
    current_transaction = transaction;
    add_record(RECORD_PMEM, offset, BUFMETA_OP, data, 0, length);
    current_transaction = previous;
    return transaction->error;
}

int orchfs_log_transaction_is_empty(
    const struct orchfs_log_transaction* transaction)
{
    return transaction != NULL && transaction->error == 0 &&
           transaction->first == NULL && transaction->allocations == NULL &&
           transaction->deallocations == NULL;
}

int orchfs_log_transaction_commit_group(
    struct orchfs_log_transaction** transactions, size_t count, int* errors)
{
    if((count != 0 && (transactions == NULL || errors == NULL)))
        return EINVAL;
    for(size_t index = 0; index < count; ++index)
    {
        if(transactions[index] == NULL ||
           current_transaction == transactions[index])
            return EINVAL;
    }
    int logged = 0;
    for(size_t index = 0; index < count; ++index)
    {
        struct orchfs_log_transaction* transaction = transactions[index];
        int error = transaction->error;
        for(struct transaction_record* record = transaction->first;
            error == 0 && record != NULL; record = record->next)
            error = append_raw_record(record);
        errors[index] = error;
        if(error == 0 && transaction->first != NULL)
            logged = 1;
    }
    if(logged)
        _mm_sfence();

    int applied = 0;
    for(size_t index = 0; index < count; ++index)
    {
        struct orchfs_log_transaction* transaction = transactions[index];
        int error = errors[index];
        if(error != 0)
            continue;
        for(struct transaction_record* record = transaction->first;
            error == 0 && record != NULL; record = record->next)
        {
            if(record->kind != RECORD_CHANGE && record->kind != RECORD_PMEM)
                continue;
            if(record->kind == RECORD_PMEM)
            {
                if(record->block < 0 ||
                   (uint64_t)record->block > (uint64_t)PMEM_LEN ||
                   (uint64_t)record->length >
                       (uint64_t)PMEM_LEN - (uint64_t)record->block)
                {
                    error = EINVAL;
                    continue;
                }
                nvm_write(record->data, record->length, record->block);
                continue;
            }
            void* memory = NULL;
            int64_t offset = 0;
            error = metadata_location(record->type, record->block,
                                      record->offset, record->length,
                                      &memory, &offset);
            if(error == 0)
                nvm_write(record->data, record->length, offset);
        }
        errors[index] = error;
        if(error == 0 && transaction->first != NULL)
            applied = 1;
    }
    if(applied)
        _mm_sfence();

    for(size_t index = 0; index < count; ++index)
    {
        struct orchfs_log_transaction* transaction = transactions[index];
        if(errors[index] != 0)
        {
            orchfs_log_transaction_abort(transaction);
            continue;
        }
        free_records(transaction);
        free_allocations(transaction, 0);
        free_deallocations(transaction, 1);
        free(transaction);
    }
    return 0;
}

int orchfs_log_transaction_commit(struct orchfs_log_transaction* transaction)
{
    if(transaction == NULL)
        return EINVAL;
    struct orchfs_log_transaction* transactions[1] = {transaction};
    int errors[1] = {0};
    const int error = orchfs_log_transaction_commit_group(
        transactions, 1, errors);
    return error != 0 ? error : errors[0];
}

void orchfs_log_transaction_abort(struct orchfs_log_transaction* transaction)
{
    if(transaction == NULL)
        return;
    if(current_transaction == transaction)
        current_transaction = NULL;
    for(struct transaction_record* record = transaction->first;
        record != NULL; record = record->next)
    {
        if(record->kind != RECORD_CHANGE)
            continue;
        void* memory = NULL;
        int64_t offset = 0;
        if(metadata_location(record->type, record->block,
                             record->offset, record->length,
                             &memory, &offset) == 0)
            nvm_read(memory, record->length, offset);
    }
    free_allocations(transaction, 1);
    free_deallocations(transaction, 0);
    free_records(transaction);
    free(transaction);
}
