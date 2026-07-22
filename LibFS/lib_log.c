#include "lib_log.h"

#include "libspace.h"
#include "meta_cache.h"
#include "req_kernel.h"
#include "../config/config.h"
#include "../config/log_config.h"
#include "../KernelFS/balloc.h"
#include "../KernelFS/device.h"
#include "../KernelFS/type.h"

#include <errno.h>
#include <immintrin.h>
#include <signal.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
    uint64_t next_txid;
    uint64_t checkpoint_generation;
    int initialized;
};

static struct journal_state journal;
static _Thread_local struct orchfs_log_transaction* current_transaction;

static uint32_t crc32c(const void* bytes, size_t length)
{
    const uint8_t* input = bytes;
    uint32_t crc = UINT32_MAX;
    while(length-- != 0)
    {
        crc ^= *input++;
        for(int bit = 0; bit < 8; ++bit)
            crc = (crc >> 1) ^ (UINT32_C(0x82f63b78) &
                    (uint32_t)-(int32_t)(crc & 1U));
    }
    return ~crc;
}

static size_t align_cacheline(size_t length)
{
    return (length + 63U) & ~(size_t)63U;
}

static void crash_at_test_point(const char* point)
{
    const char* requested = getenv("ORCHFS_WAL_CRASH_POINT");
    if(requested == NULL || strcmp(requested, point) != 0)
        return;
    (void)kill(getpid(), SIGKILL);
    _exit(128 + SIGKILL);
}

static int valid_extent_block(int type, int64_t block)
{
    if(block < 0)
        return 0;
    uint64_t capacity = type == INODE_EXT ? MAX_INODE_NUM
        : type == IDXND_EXT ? MAX_INDEX_NUM
        : type == VIRND_EXT ? MAX_VIRND_NUM
        : type == BUFMETA_EXT ? MAX_BUFMETA_NUM
        : type == PAGE_EXT ? MAX_PAGE_NUM
        : type == BLOCK_EXT ? MAX_BLOCK_NUM : 0;
    return (uint64_t)block < capacity;
}

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

static uint32_t checkpoint_checksum(
    const struct orchfs_disk_checkpoint* checkpoint)
{
    struct orchfs_disk_checkpoint copy = *checkpoint;
    copy.checksum = 0;
    return crc32c(&copy, sizeof(copy));
}

static int valid_checkpoint(const struct orchfs_disk_checkpoint* checkpoint)
{
    return checkpoint->generation != 0 &&
           checkpoint->journal_tail <= ORCHFS_JOURNAL_BYTES &&
           checkpoint->checksum == checkpoint_checksum(checkpoint);
}

static int read_superblock(orch_super_blk_t* superblock)
{
    if(superblock == NULL)
        return EINVAL;
    unsigned char bytes[ORCH_SUPER_BLK_SIZE];
    nvm_read(bytes, sizeof(bytes), OFFSET_SUPER_BLK);
    memcpy(superblock, bytes, sizeof(*superblock));
    return 0;
}

int orchfs_log_validate_format(void)
{
    orch_super_blk_t superblock;
    read_superblock(&superblock);
    if(superblock.magic_num[0] != ORCH_MAGIC_NUM ||
       superblock.magic_num[1] != ORCH_MAGIC_NUM ||
       superblock.magic_num[2] != ORCH_MAGIC_NUM)
        return EINVAL;
    if(superblock.format_version != ORCHFS_DISK_FORMAT_VERSION ||
       (superblock.feature_flags & ORCHFS_DISK_FEATURE_WAL) == 0)
        return EOPNOTSUPP;
    return 0;
}

static int latest_checkpoint(const orch_super_blk_t* superblock,
                             struct orchfs_disk_checkpoint* output)
{
    const int first = valid_checkpoint(&superblock->checkpoints[0]);
    const int second = valid_checkpoint(&superblock->checkpoints[1]);
    if(!first && !second)
        return EIO;
    *output = !second || (first && superblock->checkpoints[0].generation >=
                                      superblock->checkpoints[1].generation)
        ? superblock->checkpoints[0] : superblock->checkpoints[1];
    return 0;
}

static int write_checkpoint(uint64_t durable_txid)
{
    if(journal.checkpoint_generation == UINT64_MAX)
        return EOVERFLOW;
    struct orchfs_disk_checkpoint checkpoint = {
        .generation = journal.checkpoint_generation + 1,
        .durable_txid = durable_txid,
        .journal_tail = 0,
    };
    checkpoint.checksum = checkpoint_checksum(&checkpoint);
    const size_t slot = (size_t)((checkpoint.generation - 1) & 1U);
    const int64_t offset = OFFSET_SUPER_BLK +
        (int64_t)offsetof(orch_super_blk_t, checkpoints) +
        (int64_t)(slot * sizeof(checkpoint));
    nvm_write(&checkpoint, sizeof(checkpoint), offset);
    _mm_sfence();
    journal.checkpoint_generation = checkpoint.generation;
    return 0;
}

static void clear_journal(void)
{
    const unsigned char empty[64] = {0};
    nvm_write((void*)empty, sizeof(empty), OFFSET_LOG);
    _mm_sfence();
}

static int record_target(const struct transaction_record* record,
                         int64_t* target)
{
    if(record->kind == RECORD_PMEM)
    {
        if(record->block < 0 || record->length < 0 ||
           (uint64_t)record->block > PMEM_LEN ||
           (uint64_t)record->length > PMEM_LEN - (uint64_t)record->block)
            return EINVAL;
        *target = record->block;
        return 0;
    }
    void* memory = NULL;
    return metadata_location(record->type, record->block, record->offset,
                             record->length, &memory, target);
}

static int add_frame_size(size_t* payload_bytes, uint32_t* record_count,
                          size_t data_bytes)
{
    if(data_bytes > SIZE_MAX - sizeof(struct orchfs_journal_record_header))
        return EOVERFLOW;
    const size_t bytes = align_cacheline(
        sizeof(struct orchfs_journal_record_header) + data_bytes);
    if(bytes > ORCHFS_JOURNAL_BYTES ||
       *payload_bytes > ORCHFS_JOURNAL_BYTES - bytes ||
       *record_count == UINT32_MAX)
        return E2BIG;
    *payload_bytes += bytes;
    ++*record_count;
    return 0;
}

static int build_frame(const struct orchfs_log_transaction* transaction,
                       uint64_t txid, unsigned char** output,
                       size_t* output_length)
{
    size_t payload_bytes = 0;
    uint32_t record_count = 0;
    for(const struct transaction_record* record = transaction->first;
        record != NULL; record = record->next)
    {
        if(record->kind != RECORD_CHANGE && record->kind != RECORD_PMEM)
            continue;
        int64_t target = 0;
        int error = record_target(record, &target);
        if(error != 0)
            return error;
        error = add_frame_size(&payload_bytes, &record_count,
                               (size_t)record->length);
        if(error != 0)
            return error;
    }
    for(const struct allocation_record* allocation = transaction->allocations;
        allocation != NULL; allocation = allocation->next)
    {
        if(!valid_extent_block(allocation->type, allocation->block))
            return EINVAL;
        int error = add_frame_size(&payload_bytes, &record_count, 0);
        if(error != 0)
            return error;
    }
    for(const struct allocation_record* allocation =
            transaction->deallocations;
        allocation != NULL; allocation = allocation->next)
    {
        if(!valid_extent_block(allocation->type, allocation->block))
            return EINVAL;
        int error = add_frame_size(&payload_bytes, &record_count, 0);
        if(error != 0)
            return error;
    }

    const size_t fixed = sizeof(struct orchfs_journal_frame_header) +
                         sizeof(struct orchfs_journal_commit);
    if(payload_bytes > ORCHFS_JOURNAL_BYTES - fixed)
        return E2BIG;
    const size_t frame_bytes = fixed + payload_bytes;
    unsigned char* frame = calloc(1, frame_bytes);
    if(frame == NULL)
        return ENOMEM;

    struct orchfs_journal_frame_header* header = (void*)frame;
    *header = (struct orchfs_journal_frame_header){
        .magic = ORCHFS_JOURNAL_MAGIC,
        .version = ORCHFS_JOURNAL_VERSION,
        .header_bytes = sizeof(*header),
        .txid = txid,
        .frame_bytes = frame_bytes,
        .record_count = record_count,
    };
    unsigned char* cursor = frame + sizeof(*header);
    for(const struct transaction_record* record = transaction->first;
        record != NULL; record = record->next)
    {
        if(record->kind != RECORD_CHANGE && record->kind != RECORD_PMEM)
            continue;
        int64_t target = 0;
        int error = record_target(record, &target);
        if(error != 0)
        {
            free(frame);
            return error;
        }
        const size_t bytes = align_cacheline(
            sizeof(struct orchfs_journal_record_header) +
            (size_t)record->length);
        struct orchfs_journal_record_header* disk_record = (void*)cursor;
        *disk_record = (struct orchfs_journal_record_header){
            .kind = ORCHFS_JOURNAL_WRITE,
            .type = (uint32_t)record->type,
            .target = target,
            .length = (uint32_t)record->length,
            .record_bytes = (uint32_t)bytes,
            .data_checksum = crc32c(record->data, (size_t)record->length),
        };
        memcpy(cursor + sizeof(*disk_record), record->data,
               (size_t)record->length);
        cursor += bytes;
    }
    for(int list = 0; list < 2; ++list)
    {
        const struct allocation_record* allocation = list == 0
            ? transaction->allocations : transaction->deallocations;
        for(; allocation != NULL; allocation = allocation->next)
        {
            struct orchfs_journal_record_header* disk_record = (void*)cursor;
            *disk_record = (struct orchfs_journal_record_header){
                .kind = list == 0 ? ORCHFS_JOURNAL_ALLOCATE
                                  : ORCHFS_JOURNAL_FREE,
                .type = (uint32_t)allocation->type,
                .target = allocation->block,
                .record_bytes = align_cacheline(sizeof(*disk_record)),
            };
            cursor += disk_record->record_bytes;
        }
    }
    header->payload_checksum = crc32c(
        frame + sizeof(*header), payload_bytes);
    header->header_checksum = 0;
    header->header_checksum = crc32c(header, sizeof(*header));
    struct orchfs_journal_commit* commit =
        (void*)(frame + frame_bytes - sizeof(*commit));
    *commit = (struct orchfs_journal_commit){
        .magic = ORCHFS_JOURNAL_COMMIT_MAGIC,
        .txid = txid,
        .frame_bytes = frame_bytes,
        .frame_checksum = crc32c(frame, frame_bytes - sizeof(*commit)),
    };
    *output = frame;
    *output_length = frame_bytes;
    return 0;
}

static int validate_frame(const unsigned char* frame, size_t frame_bytes)
{
    if(frame_bytes < sizeof(struct orchfs_journal_frame_header) +
                         sizeof(struct orchfs_journal_commit) ||
       (frame_bytes & 63U) != 0)
        return EINVAL;
    const struct orchfs_journal_frame_header* header = (const void*)frame;
    if(header->magic != ORCHFS_JOURNAL_MAGIC ||
       header->version != ORCHFS_JOURNAL_VERSION ||
       header->header_bytes != sizeof(*header) ||
       header->frame_bytes != frame_bytes)
        return EINVAL;
    struct orchfs_journal_frame_header header_copy = *header;
    const uint32_t header_checksum = header_copy.header_checksum;
    header_copy.header_checksum = 0;
    if(header_checksum != crc32c(&header_copy, sizeof(header_copy)))
        return EINVAL;
    const struct orchfs_journal_commit* commit = (const void*)(
        frame + frame_bytes - sizeof(*commit));
    if(commit->magic != ORCHFS_JOURNAL_COMMIT_MAGIC ||
       commit->txid != header->txid || commit->frame_bytes != frame_bytes ||
       commit->frame_checksum != crc32c(frame, frame_bytes - sizeof(*commit)))
        return EINVAL;
    const size_t payload_bytes = frame_bytes - sizeof(*header) - sizeof(*commit);
    if(header->payload_checksum != crc32c(frame + sizeof(*header),
                                         payload_bytes))
        return EINVAL;

    const unsigned char* cursor = frame + sizeof(*header);
    const unsigned char* payload_end = cursor + payload_bytes;
    for(uint32_t index = 0; index < header->record_count; ++index)
    {
        if((size_t)(payload_end - cursor) <
           sizeof(struct orchfs_journal_record_header))
            return EINVAL;
        const struct orchfs_journal_record_header* record = (const void*)cursor;
        const size_t expected = align_cacheline(sizeof(*record) +
                                                (size_t)record->length);
        if(record->record_bytes != expected || expected > (size_t)(payload_end - cursor))
            return EINVAL;
        if(record->kind == ORCHFS_JOURNAL_WRITE)
        {
            if(record->target < 0 || record->offset != 0 ||
               (uint64_t)record->target > PMEM_LEN ||
               record->length > PMEM_LEN - (uint64_t)record->target ||
               record->data_checksum != crc32c(cursor + sizeof(*record),
                                                record->length))
                return EINVAL;
        }
        else if(record->kind == ORCHFS_JOURNAL_ALLOCATE ||
                record->kind == ORCHFS_JOURNAL_FREE)
        {
            if(record->length != 0 ||
               !valid_extent_block((int)record->type, record->target))
                return EINVAL;
        }
        else
            return EINVAL;
        cursor += expected;
    }
    return cursor == payload_end ? 0 : EINVAL;
}

static int apply_frame(const unsigned char* frame, int recovering)
{
    const struct orchfs_journal_frame_header* header = (const void*)frame;
    const unsigned char* cursor = frame + sizeof(*header);
    for(uint32_t index = 0; index < header->record_count; ++index)
    {
        const struct orchfs_journal_record_header* record = (const void*)cursor;
        int error = 0;
        if(record->kind == ORCHFS_JOURNAL_WRITE)
            nvm_write((void*)(cursor + sizeof(*record)), record->length,
                      record->target);
        else
            error = recovering
                ? orchfs_bitmap_replay_change(
                      (int)record->type, record->target,
                      record->kind == ORCHFS_JOURNAL_ALLOCATE)
                : orchfs_bitmap_persist_change(
                      (int)record->type, record->target,
                      record->kind == ORCHFS_JOURNAL_ALLOCATE);
        if(error != 0)
            return error;
        cursor += record->record_bytes;
    }
    _mm_sfence();
    return 0;
}

int orchfs_log_recover(void)
{
    int error = orchfs_log_validate_format();
    if(error != 0)
        return error;
    orch_super_blk_t superblock;
    read_superblock(&superblock);
    struct orchfs_disk_checkpoint checkpoint;
    error = latest_checkpoint(&superblock, &checkpoint);
    if(error != 0)
        return error;
    journal.checkpoint_generation = checkpoint.generation;
    uint64_t durable_txid = checkpoint.durable_txid;
    uint64_t cursor = 0;
    while(cursor + sizeof(struct orchfs_journal_frame_header) <=
          ORCHFS_JOURNAL_BYTES)
    {
        struct orchfs_journal_frame_header header;
        nvm_read(&header, sizeof(header), OFFSET_LOG + cursor);
        if(header.magic != ORCHFS_JOURNAL_MAGIC ||
           header.frame_bytes < sizeof(header) +
                                sizeof(struct orchfs_journal_commit) ||
           (header.frame_bytes & 63U) != 0 ||
           header.frame_bytes > ORCHFS_JOURNAL_BYTES - cursor)
            break;
        unsigned char* frame = malloc((size_t)header.frame_bytes);
        if(frame == NULL)
            return ENOMEM;
        nvm_read(frame, (int64_t)header.frame_bytes, OFFSET_LOG + cursor);
        error = validate_frame(frame, (size_t)header.frame_bytes);
        if(error != 0)
        {
            free(frame);
            break;
        }
        error = apply_frame(frame, 1);
        if(error == 0 && header.txid > durable_txid)
            durable_txid = header.txid;
        cursor += header.frame_bytes;
        free(frame);
        if(error != 0)
            return error;
    }
    orchfs_bitmap_recompute_counts();
    error = write_checkpoint(durable_txid);
    if(error != 0)
        return error;
    clear_journal();
    if(durable_txid == UINT64_MAX)
        return EOVERFLOW;
    journal.next_txid = durable_txid + 1;
    journal.initialized = 1;
    return 0;
}

int init_mem_log(void)
{
    return journal.initialized ? 0 : orchfs_log_recover();
}

void free_mem_log(void)
{
    _mm_sfence();
    journal.initialized = 0;
}

int orchfs_log_sync(void)
{
    if(!journal.initialized)
        return EIO;
    _mm_sfence();
    return 0;
}

int orchfs_log_transaction_create(struct orchfs_log_transaction** output)
{
    if(output == NULL)
        return EINVAL;
    if(!journal.initialized)
        return EIO;
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

static int commit_one(struct orchfs_log_transaction* transaction)
{
    if(!journal.initialized)
        return EIO;
    if(orchfs_log_transaction_is_empty(transaction))
        return 0;
    if(journal.next_txid == 0 || journal.next_txid == UINT64_MAX ||
       journal.checkpoint_generation == UINT64_MAX)
        return EOVERFLOW;
    unsigned char* frame = NULL;
    size_t frame_bytes = 0;
    int error = build_frame(transaction, journal.next_txid,
                            &frame, &frame_bytes);
    if(error != 0)
        return error;

    const size_t commit_bytes = sizeof(struct orchfs_journal_commit);
    nvm_write(frame, (int64_t)(frame_bytes - commit_bytes), OFFSET_LOG);
    _mm_sfence();
    crash_at_test_point("after-body");
    nvm_write(frame + frame_bytes - commit_bytes, (int64_t)commit_bytes,
              OFFSET_LOG + frame_bytes - commit_bytes);
    _mm_sfence();
    crash_at_test_point("after-commit");

    error = apply_frame(frame, 0);
    if(error == 0)
        crash_at_test_point("after-home");
    if(error == 0)
        error = write_checkpoint(journal.next_txid);
    if(error == 0)
    {
        clear_journal();
        ++journal.next_txid;
    }
    free(frame);
    return error;
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
    for(size_t index = 0; index < count; ++index)
    {
        struct orchfs_log_transaction* transaction = transactions[index];
        errors[index] = transaction->error != 0
            ? transaction->error : commit_one(transaction);
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
