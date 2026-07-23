#include "../LibFS/lib_log.h"
#include "../LibFS/libspace.h"
#include "../LibFS/meta_cache.h"
#include "../KernelFS/balloc.h"
#include "../KernelFS/device.h"
#include "../KernelFS/type.h"
#include "../config/log_config.h"
#include "orchfs/crc32c.h"

#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>

struct shared_media
{
    unsigned char superblock[ORCH_SUPER_BLK_SIZE];
    unsigned char journal[ORCHFS_JOURNAL_BYTES];
    unsigned char inode[ORCH_INODE_SIZE];
    unsigned char durable_bits[END_BMP_ID + 1][64];
};

static struct shared_media* media;
static struct orchfs_inode_disk inode_cache;
static unsigned char memory_bits[END_BMP_ID + 1][64];

static void fail(const char* message)
{
    fprintf(stderr, "journal_recovery_test: %s\n", message);
    exit(1);
}

static void test_crc32c_contract(void)
{
    static const unsigned char input[] = "123456789";
    const uint32_t expected = UINT32_C(0xe3069283);
    if(orchfs_crc32c_software(input, sizeof(input) - 1) != expected ||
       orchfs_crc32c(input, sizeof(input) - 1) != expected)
        fail("CRC32C fixed vector changed");
#if defined(__x86_64__)
    if(__builtin_cpu_supports("sse4.2") &&
       orchfs_crc32c_hardware(input, sizeof(input) - 1) != expected)
        fail("hardware CRC32C differs from the on-media contract");
#endif
}

static void initialize_media(void)
{
    memset(media, 0, sizeof(*media));
    orch_super_blk_pt superblock = (void*)media->superblock;
    superblock->root_inode = 0;
    for(int index = 0; index < 3; ++index)
        superblock->magic_num[index] = ORCH_MAGIC_NUM;
    superblock->format_version = ORCHFS_DISK_FORMAT_VERSION;
    superblock->feature_flags = ORCHFS_DISK_FEATURE_WAL;
    superblock->checkpoints[0].generation = 1;
    superblock->checkpoints[0].checksum = 0;
    superblock->checkpoints[0].checksum = orchfs_crc32c(
        &superblock->checkpoints[0], sizeof(superblock->checkpoints[0]));
    memset(&inode_cache, 0, sizeof(inode_cache));
    inode_cache.i_number = 0;
    memcpy(media->inode, &inode_cache, sizeof(inode_cache));
    memset(memory_bits, 0, sizeof(memory_bits));
}

static unsigned char* media_range(int64_t offset, int64_t length)
{
    if(offset < 0 || length < 0)
        fail("negative media range");
    if((uint64_t)offset <= ORCH_SUPER_BLK_SIZE &&
       (uint64_t)length <= ORCH_SUPER_BLK_SIZE - (uint64_t)offset)
        return media->superblock + offset;
    if((uint64_t)offset >= OFFSET_LOG &&
       (uint64_t)offset - OFFSET_LOG <= ORCHFS_JOURNAL_BYTES &&
       (uint64_t)length <= ORCHFS_JOURNAL_BYTES -
                           ((uint64_t)offset - OFFSET_LOG))
        return media->journal + ((uint64_t)offset - OFFSET_LOG);
    if((uint64_t)offset >= OFFSET_INODE &&
       (uint64_t)offset - OFFSET_INODE <= ORCH_INODE_SIZE &&
       (uint64_t)length <= ORCH_INODE_SIZE -
                           ((uint64_t)offset - OFFSET_INODE))
        return media->inode + ((uint64_t)offset - OFFSET_INODE);
    fail("unexpected media range");
    return NULL;
}

void nvm_read(void* destination, int64_t length, int64_t offset)
{
    memcpy(destination, media_range(offset, length), (size_t)length);
}

void nvm_write(void* source, int64_t length, int64_t offset)
{
    memcpy(media_range(offset, length), source, (size_t)length);
}

void* inodeid_to_memaddr(int64_t inode)
{
    return inode == 0 ? &inode_cache : NULL;
}

void* indexid_to_memaddr(int64_t inode, int64_t index, int create)
{
    (void)inode;
    (void)index;
    (void)create;
    return NULL;
}

void* virnodeid_to_memaddr(int64_t inode, int64_t node, int create)
{
    (void)inode;
    (void)node;
    (void)create;
    return NULL;
}

static int bitmap_change(int type, int64_t block, int allocated,
                         int replay)
{
    if(type < START_BMP_ID || type > END_BMP_ID || block < 0 || block >= 512)
        return EINVAL;
    const size_t byte = (size_t)block / 8;
    const unsigned char mask = (unsigned char)(1U << (7U - block % 8));
    if(allocated)
        media->durable_bits[type][byte] |= mask;
    else
        media->durable_bits[type][byte] &= (unsigned char)~mask;
    if(replay)
    {
        if(allocated)
            memory_bits[type][byte] |= mask;
        else
            memory_bits[type][byte] &= (unsigned char)~mask;
    }
    return 0;
}

int orchfs_bitmap_persist_change(int type, int64_t block, int allocated)
{
    return bitmap_change(type, block, allocated, 0);
}

int orchfs_bitmap_replay_change(int type, int64_t block, int allocated)
{
    return bitmap_change(type, block, allocated, 1);
}

void orchfs_bitmap_recompute_counts(void) {}

void release_inode(int64_t block) { (void)block; }
void release_index_node(int64_t block) { (void)block; }
void release_virindex_node(int64_t block) { (void)block; }
void release_buffer_metadata(int64_t block) { (void)block; }
void release_nvm_page(int64_t block) { (void)block; }
void release_ssd_block(int64_t block) { (void)block; }

static int bit_is_set(const unsigned char bits[][64], int type, int block)
{
    return (bits[type][block / 8] &
            (unsigned char)(1U << (7U - block % 8))) != 0;
}

static void run_crashing_transaction(const char* point, unsigned char marker,
                                     int block)
{
    pid_t child = fork();
    if(child < 0)
        fail("fork failed");
    if(child == 0)
    {
        if(setenv("ORCHFS_WAL_CRASH_POINT", point, 1) != 0)
            _exit(2);
        struct orchfs_log_transaction* transaction = NULL;
        if(orchfs_log_transaction_create(&transaction) != 0)
            _exit(3);
        orchfs_log_transaction_bind(transaction);
        memset(inode_cache.reserved, marker, sizeof(inode_cache.reserved));
        write_change_log(0, INODE_OP, &inode_cache, 0, ORCH_INODE_SIZE);
        if(orchfs_log_transaction_record_allocation(
               transaction, PAGE_EXT, block) != 0)
            _exit(4);
        orchfs_log_transaction_unbind(transaction);
        (void)orchfs_log_transaction_commit(transaction);
        _exit(5);
    }
    int status = 0;
    if(waitpid(child, &status, 0) != child || !WIFSIGNALED(status) ||
       WTERMSIG(status) != SIGKILL)
        fail("transaction did not stop at the requested SIGKILL point");
}

int main(void)
{
    test_crc32c_contract();
    media = mmap(NULL, sizeof(*media), PROT_READ | PROT_WRITE,
                 MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if(media == MAP_FAILED)
        fail("shared media mmap failed");
    initialize_media();
    if(orchfs_log_validate_format() != 0 || orchfs_log_recover() != 0)
        fail("format validation or initial recovery failed");

    run_crashing_transaction("after-commit", 0x5a, 7);
    if(media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0 ||
       bit_is_set(media->durable_bits, PAGE_EXT, 7))
        fail("home state changed before recovery");
    free_mem_log();
    memcpy(&inode_cache, media->inode, sizeof(inode_cache));
    memset(memory_bits, 0, sizeof(memory_bits));
    if(orchfs_log_recover() != 0)
        fail("committed frame recovery failed");
    if(media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0x5a ||
       !bit_is_set(media->durable_bits, PAGE_EXT, 7) ||
       !bit_is_set(memory_bits, PAGE_EXT, 7))
        fail("recovery did not restore metadata and allocation state");

    run_crashing_transaction("after-home", 0x3c, 9);
    if(media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0x3c ||
       !bit_is_set(media->durable_bits, PAGE_EXT, 9))
        fail("home state was not durable before checkpoint recovery");
    free_mem_log();
    memcpy(&inode_cache, media->inode, sizeof(inode_cache));
    memset(memory_bits, 0, sizeof(memory_bits));
    if(orchfs_log_recover() != 0 ||
       media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0x3c ||
       !bit_is_set(media->durable_bits, PAGE_EXT, 9) ||
       !bit_is_set(memory_bits, PAGE_EXT, 9))
        fail("after-home recovery was not idempotent");

    run_crashing_transaction("after-body", 0xa5, 8);
    free_mem_log();
    memcpy(&inode_cache, media->inode, sizeof(inode_cache));
    memset(memory_bits, 0, sizeof(memory_bits));
    if(orchfs_log_recover() != 0)
        fail("torn frame recovery failed");
    if(media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0x3c ||
       bit_is_set(media->durable_bits, PAGE_EXT, 8))
        fail("torn transaction became visible");

    free_mem_log();
    if(orchfs_log_recover() != 0 ||
       media->inode[offsetof(struct orchfs_inode_disk, reserved)] != 0x3c ||
       !bit_is_set(media->durable_bits, PAGE_EXT, 7) ||
       !bit_is_set(media->durable_bits, PAGE_EXT, 9))
        fail("idempotent recovery failed");

    orch_super_blk_pt superblock = (void*)media->superblock;
    superblock->format_version = 1;
    if(orchfs_log_validate_format() != EOPNOTSUPP)
        fail("legacy format was not rejected");
    printf("journal_recovery_test: PASS\n");
    return 0;
}
