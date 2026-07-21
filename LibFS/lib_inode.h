#ifndef LIB_INODE_H
#define LIB_INODE_H

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/statfs.h>
#include <sys/vfs.h>
#include <linux/magic.h>
#include <linux/falloc.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <dirent.h>
#include <string.h>
#include <error.h>
#include <errno.h>
#include <inttypes.h>
#include <time.h>
#include <x86intrin.h>
#include <stdarg.h> 

#include "../include/orchfs/disk_format.h"


#define ino_id_t                   int64_t
#define nlink_t                    int64_t
#define fsize_t                    int64_t                    
#define uid_t                      int32_t
#define gid_t                      int32_t
#define mode_t                     int32_t
#define ftype_t                    int32_t

#define DIR_FILE                   0b00000001
#define SIMPLE_FILE                0b00000010

typedef struct orchfs_inode_disk orch_inode_t;
typedef orch_inode_t* orch_inode_pt;

/* Initialize an inode and return the inode number */
ino_id_t inode_create(ftype_t i_type);

/* Given the inode ID, delete the inode and all its related caches */
int delete_inode(ino_id_t delete_id);

/* Given the inode ID, synchronize all relevant information with it */
void sync_inode_and_index(ino_id_t sync_ino_id);

/* Given inode ID, change file size */
void inode_file_resize(orch_inode_pt inode_pt, fsize_t new_file_size);


void inode_change_file_size(orch_inode_pt inode_pt, fsize_t new_file_size);

#endif
