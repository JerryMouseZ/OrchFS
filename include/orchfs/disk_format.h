#ifndef ORCHFS_DISK_FORMAT_H
#define ORCHFS_DISK_FORMAT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ORCHFS_DISK_FORMAT_VERSION UINT64_C(2)
#define ORCHFS_DISK_FEATURE_WAL UINT64_C(1)

#define ORCHFS_JOURNAL_MAGIC UINT64_C(0x4f52434857414c32)
#define ORCHFS_JOURNAL_COMMIT_MAGIC UINT64_C(0x434f4d4d49543221)
#define ORCHFS_JOURNAL_VERSION UINT32_C(1)
#define ORCHFS_JOURNAL_BYTES (UINT64_C(64) << 20)
#define ORCHFS_INODE_FLAG_ORPHAN UINT8_C(1)

enum orchfs_journal_record_kind {
    ORCHFS_JOURNAL_WRITE = 1,
    ORCHFS_JOURNAL_ALLOCATE = 2,
    ORCHFS_JOURNAL_FREE = 3,
};

struct orchfs_disk_checkpoint {
    uint64_t generation;
    uint64_t durable_txid;
    uint64_t journal_tail;
    uint32_t checksum;
    uint32_t reserved;
};

struct orchfs_journal_frame_header {
    uint64_t magic;
    uint32_t version;
    uint32_t header_bytes;
    uint64_t txid;
    uint64_t frame_bytes;
    uint32_t record_count;
    uint32_t payload_checksum;
    uint32_t header_checksum;
    uint8_t reserved[20];
};

struct orchfs_journal_record_header {
    uint32_t kind;
    uint32_t type;
    int64_t target;
    int32_t offset;
    uint32_t length;
    uint32_t record_bytes;
    uint32_t data_checksum;
};

struct orchfs_journal_commit {
    uint64_t magic;
    uint64_t txid;
    uint64_t frame_bytes;
    uint32_t frame_checksum;
    uint32_t reserved32;
    uint8_t reserved[32];
};

/*
 * Persistent inode format.
 *
 * This is intentionally independent of libc and pthread ABI types.  Older
 * images stored a process-local spin-lock object at byte 96; that byte range
 * was never filesystem state.  It is now part of a fixed reserved area, so all
 * meaningful fields keep their existing offsets while the complete on-media
 * object has an explicit 512-byte size.
 */
struct orchfs_disk_timestamp {
    int64_t tv_sec;
    int64_t tv_nsec;
};

struct orchfs_inode_disk {
    int64_t i_number;
    int64_t i_size;
    int64_t i_idxroot;
    int64_t i_nlink;

    int32_t i_uid;
    int32_t i_gid;
    int32_t i_mode;
    int32_t i_type;

    struct orchfs_disk_timestamp i_atim;
    struct orchfs_disk_timestamp i_mtim;
    struct orchfs_disk_timestamp i_ctim;

    uint8_t reserved[416];
};

#ifdef __cplusplus
}

static_assert(sizeof(orchfs_disk_timestamp) == 16);
static_assert(sizeof(orchfs_inode_disk) == 512);
static_assert(sizeof(orchfs_disk_checkpoint) == 32);
static_assert(sizeof(orchfs_journal_frame_header) == 64);
static_assert(sizeof(orchfs_journal_record_header) == 32);
static_assert(sizeof(orchfs_journal_commit) == 64);
static_assert(offsetof(orchfs_inode_disk, i_number) == 0);
static_assert(offsetof(orchfs_inode_disk, i_size) == 8);
static_assert(offsetof(orchfs_inode_disk, i_idxroot) == 16);
static_assert(offsetof(orchfs_inode_disk, i_nlink) == 24);
static_assert(offsetof(orchfs_inode_disk, i_uid) == 32);
static_assert(offsetof(orchfs_inode_disk, i_gid) == 36);
static_assert(offsetof(orchfs_inode_disk, i_mode) == 40);
static_assert(offsetof(orchfs_inode_disk, i_type) == 44);
static_assert(offsetof(orchfs_inode_disk, i_atim) == 48);
static_assert(offsetof(orchfs_inode_disk, i_mtim) == 64);
static_assert(offsetof(orchfs_inode_disk, i_ctim) == 80);
static_assert(offsetof(orchfs_inode_disk, reserved) == 96);
#else
_Static_assert(sizeof(struct orchfs_disk_timestamp) == 16,
               "disk timestamp must be 16 bytes");
_Static_assert(sizeof(struct orchfs_inode_disk) == 512,
               "disk inode must be 512 bytes");
_Static_assert(sizeof(struct orchfs_disk_checkpoint) == 32,
               "checkpoint slot must be 32 bytes");
_Static_assert(sizeof(struct orchfs_journal_frame_header) == 64,
               "journal frame header must be 64 bytes");
_Static_assert(sizeof(struct orchfs_journal_record_header) == 32,
               "journal record header must be 32 bytes");
_Static_assert(sizeof(struct orchfs_journal_commit) == 64,
               "journal commit must be 64 bytes");
_Static_assert(offsetof(struct orchfs_inode_disk, i_number) == 0,
               "i_number offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_size) == 8,
               "i_size offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_idxroot) == 16,
               "i_idxroot offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_nlink) == 24,
               "i_nlink offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_uid) == 32,
               "i_uid offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_gid) == 36,
               "i_gid offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_mode) == 40,
               "i_mode offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_type) == 44,
               "i_type offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_atim) == 48,
               "i_atim offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_mtim) == 64,
               "i_mtim offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, i_ctim) == 80,
               "i_ctim offset changed");
_Static_assert(offsetof(struct orchfs_inode_disk, reserved) == 96,
               "reserved offset changed");
#endif

#endif
