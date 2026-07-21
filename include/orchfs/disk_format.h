#ifndef ORCHFS_DISK_FORMAT_H
#define ORCHFS_DISK_FORMAT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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
