#include "../LibFS/lib_dir.h"
#include "../LibFS/lib_inode.h"
#include "../LibFS/orchfs.h"
#include "../LibFS/runtime.h"
#include "../KernelFS/device.h"
#include "../KernelFS/type.h"
#include "../config/config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum close_event {
	EVENT_STOP_WORKERS = 1,
	EVENT_FREE_LOG,
	EVENT_CLOSE_CACHE,
	EVENT_RETURN_EXTENTS,
	EVENT_FREE_RUNTIME,
	EVENT_FREE_SOCKET,
	EVENT_CLOSE_SHM,
};

static orch_inode_t file_inode;
static orch_inode_t directory_inode;
static int path_exists;
static int release_count;
static int write_count;
static int lock_count;
static int unlock_count;
static int duplicate_directory_allocation;
static int directory_allocation_cursor = -1;
static int rename_test_active;
static int rename_distinct_parents;
static const char *rename_missing_parent;
static int rename_core_result;
static int rename_core_errno;
static int rename_core_calls;
static int rename_path_calls;
static char rename_paths[2][4096];
static char renamed_from[ORCH_DIRENT_NAME_MAX + 1];
static char renamed_to[ORCH_DIRENT_NAME_MAX + 1];
static int close_events[16];
static size_t close_event_count;

enum {
	TEST_FILE_FD = 7,
	TEST_DIRECTORY_FD = 8,
	TEST_DUPLICATE_DIRECTORY_FD = 9,
};

static void require(int condition, const char *message)
{
	if(!condition)
	{
		fprintf(stderr, "legacy_lib_func_test: %s\n", message);
		exit(1);
	}
}

static void record_close_event(int event)
{
	require(close_event_count < sizeof(close_events) / sizeof(close_events[0]),
		"close event buffer overflow");
	close_events[close_event_count++] = event;
}

/* Minimal legacy-core dependencies retained by --gc-sections. */
void init_lib_socket_info(void) {}
void free_lib_socket_info(void) { record_close_event(EVENT_FREE_SOCKET); }
void init_runtime_info(void) {}
void free_runtime_info(void) { record_close_event(EVENT_FREE_RUNTIME); }
int init_all_ext(void) { return 0; }
int return_all_ext(void)
{
	record_close_event(EVENT_RETURN_EXTENTS);
	return 0;
}
void init_metadata_cache(void) {}
void close_metadata_cache(void) { record_close_event(EVENT_CLOSE_CACHE); }
void init_io_thread_pool(void) {}
void destroy_io_thread_pool(void) { record_close_event(EVENT_STOP_WORKERS); }
void init_lib_shm(int reg_id) { (void)reg_id; }
void close_lib_shm(void) { record_close_event(EVENT_CLOSE_SHM); }
void init_mem_log(void) {}
void free_mem_log(void) { record_close_event(EVENT_FREE_LOG); }
void device_init(void) {}
void device_close(void) {}

void read_data_from_devs(void *destination, int64_t length, int64_t offset)
{
	require(offset == 0 && length == ORCH_SUPER_BLK_SIZE,
		"unexpected superblock read");
	orch_super_blk_pt superblock = destination;
	memset(superblock, 0, (size_t)length);
	superblock->magic_num[0] = ORCH_MAGIC_NUM;
	superblock->magic_num[1] = ORCH_MAGIC_NUM;
	superblock->magic_num[2] = ORCH_MAGIC_NUM;
	superblock->root_inode = 1;
}

void change_current_dir_ino(int64_t root_inode, int64_t inode)
{
	require(root_inode == 1 && inode == 1, "unexpected root inode");
}

inode_id_t path_to_inode(char *pathname, int create_flag)
{
	if(rename_test_active && create_flag == NOT_CREATE_PATH)
	{
		require(rename_path_calls < 2, "rename resolved too many parents");
		strncpy(rename_paths[rename_path_calls], pathname,
			sizeof(rename_paths[rename_path_calls]) - 1);
		rename_path_calls++;
		if(rename_missing_parent != NULL &&
		   strcmp(pathname, rename_missing_parent) == 0)
			return -1;
		if(rename_distinct_parents && strcmp(pathname, "/right") == 0)
			return 43;
		return 42;
	}
	if(create_flag == CREATE_PATH)
	{
		path_exists = 1;
		return 42;
	}
	return path_exists ? 42 : -1;
}

int file_rename(inode_id_t parent_inode, const char *old_name,
	const char *new_name)
{
	require(parent_inode == 42, "rename used the wrong parent inode");
	rename_core_calls++;
	strcpy(renamed_from, old_name);
	strcpy(renamed_to, new_name);
	if(rename_core_result != 0)
	{
		errno = rename_core_errno;
		return -1;
	}
	return 0;
}

inode_id_t path_to_inode_fromdir(inode_id_t start_inode, char *pathname,
	int create_flag)
{
	require(start_inode == directory_inode.i_number,
		"openat used the wrong directory inode");
	return path_to_inode(pathname, create_flag);
}

int64_t get_unused_fd(int64_t inode_id, int mode)
{
	require(inode_id == 42, "unexpected inode allocation");
	if(directory_allocation_cursor >= 0)
	{
		for(int index = 0; index < ORCH_MAX_FD; index++)
		{
			int candidate =
				(directory_allocation_cursor + index) % ORCH_MAX_FD;
			if(orch_rt.fd_info[candidate].used_flag == FD_NOT_USED)
			{
				directory_allocation_cursor = -1;
				orch_rt.fd_info[candidate].used_flag = FD_USED;
				orch_rt.fd_info[candidate].flags = mode;
				orch_rt.fd_info[candidate].rw_offset = 0;
				return candidate;
			}
		}
		errno = EMFILE;
		return -1;
	}
	if(duplicate_directory_allocation)
	{
		duplicate_directory_allocation = 0;
		orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].used_flag = FD_USED;
		orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].flags = mode;
		orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].rw_offset = 0;
		return TEST_DUPLICATE_DIRECTORY_FD;
	}
	orch_rt.fd_info[TEST_FILE_FD].used_flag = FD_USED;
	orch_rt.fd_info[TEST_FILE_FD].flags = mode;
	orch_rt.fd_info[TEST_FILE_FD].rw_offset = 0;
	return TEST_FILE_FD;
}

void release_fd(int fd)
{
	require(fd >= 0 && fd < ORCH_MAX_FD,
		"released the wrong fd");
	orch_rt.fd_info[fd].used_flag = FD_NOT_USED;
	release_count++;
}

orch_inode_pt fd_to_inodept(int fd)
{
	if(fd == TEST_DIRECTORY_FD)
		return &directory_inode;
	require(fd >= 0 && fd < ORCH_MAX_FD &&
		orch_rt.fd_info[fd].used_flag == FD_USED,
		"looked up an unexpected fd");
	return &file_inode;
}

int64_t fd_to_inodeid(int fd)
{
	return fd_to_inodept(fd)->i_number;
}

int64_t get_fd_file_offset(int fd)
{
	require(fd >= 0 && fd < ORCH_MAX_FD &&
		orch_rt.fd_info[fd].used_flag == FD_USED,
		"read offset from an unexpected fd");
	return orch_rt.fd_info[fd].rw_offset;
}

void change_fd_file_offset(int fd, int64_t offset)
{
	require(fd >= 0 && fd < ORCH_MAX_FD &&
		orch_rt.fd_info[fd].used_flag == FD_USED,
		"changed offset on an unexpected fd");
	orch_rt.fd_info[fd].rw_offset = offset;
}

void file_lock_wrlock(int fd)
{
	require(fd == TEST_FILE_FD, "locked an unexpected fd");
	lock_count++;
}

void file_lock_unlock(int fd)
{
	require(fd == TEST_FILE_FD, "unlocked an unexpected fd");
	unlock_count++;
}

void inode_change_file_size(orch_inode_pt inode, fsize_t new_size)
{
	require(inode == &file_inode, "resized an unexpected inode");
	inode->i_size = new_size;
}

void write_into_file(int fd, int64_t offset, int64_t length, void *buffer)
{
	require(fd == TEST_FILE_FD && offset == file_inode.i_size,
		"unexpected extension write range");
	const unsigned char *bytes = buffer;
	for(int64_t index = 0; index < length; index++)
		require(bytes[index] == 0, "truncate extension was not zero-filled");
	write_count++;
}

static void reset_file(int flags, int type, int64_t size, int64_t offset)
{
	memset(&file_inode, 0, sizeof(file_inode));
	file_inode.i_number = 42;
	file_inode.i_type = type;
	file_inode.i_size = size;
	orch_rt.fd_info[TEST_FILE_FD].used_flag = FD_USED;
	orch_rt.fd_info[TEST_FILE_FD].flags = flags;
	orch_rt.fd_info[TEST_FILE_FD].rw_offset = offset;
	orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].used_flag = FD_NOT_USED;
	duplicate_directory_allocation = 0;
	directory_allocation_cursor = -1;
	lock_count = 0;
	unlock_count = 0;
	write_count = 0;
}

static void test_fdopendir_anchors_open_inode(void)
{
	reset_file(O_RDONLY, DIR_FILE, DIRENT_SIZE * 3, 17);
	duplicate_directory_allocation = 1;
	release_count = 0;
	DIR *directory = orchfs_fdopendir(TEST_FILE_FD);
	require(directory != NULL &&
		orchfs_dirfd(directory) == TEST_DUPLICATE_DIRECTORY_FD,
		"fdopendir did not return the duplicated descriptor");
	require(orch_rt.fd_info[TEST_FILE_FD].used_flag == FD_USED,
		"fdopendir consumed its source descriptor");
	require(orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].used_flag == FD_USED,
		"fdopendir did not allocate an independent descriptor");
	require(orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].flags == O_RDONLY,
		"fdopendir did not create a read-only directory cursor");
	require(orch_rt.fd_info[TEST_DUPLICATE_DIRECTORY_FD].rw_offset ==
		DIRENT_SIZE * 2, "fdopendir initialized the wrong cursor offset");
	require(orchfs_fd_inode_id(TEST_DUPLICATE_DIRECTORY_FD) ==
		orchfs_fd_inode_id(TEST_FILE_FD),
		"fdopendir changed the already-resolved inode");
	require(orchfs_closedir(directory) == 0 && release_count == 1,
		"closing the directory cursor did not release its descriptor");
	require(orch_rt.fd_info[TEST_FILE_FD].used_flag == FD_USED,
		"closing the directory cursor consumed its source descriptor");

	reset_file(O_RDONLY, SIMPLE_FILE, 0, 0);
	errno = 0;
	require(orchfs_fdopendir(TEST_FILE_FD) == NULL && errno == ENOTDIR,
		"fdopendir accepted a non-directory inode");
	errno = 0;
	require(orchfs_fdopendir(-1) == NULL && errno == EBADF,
		"fdopendir accepted an invalid descriptor");

	reset_file(O_RDONLY, SIMPLE_FILE, 0, 0);
	path_exists = 1;
	release_count = 0;
	errno = 0;
	require(orchfs_opendir("regular-file") == NULL && errno == ENOTDIR,
		"opendir reported the wrong error for a non-directory inode");
	require(release_count == 1 &&
		orch_rt.fd_info[TEST_FILE_FD].used_flag == FD_NOT_USED,
		"failed opendir leaked its allocated descriptor");
}

static void test_directory_fd_zero_encoding(void)
{
	/* The first allocation starts at descriptor zero. */
	reset_file(O_RDONLY, DIR_FILE, DIRENT_SIZE * 3, 0);
	path_exists = 1;
	orch_rt.fd_info[0].used_flag = FD_NOT_USED;
	directory_allocation_cursor = 0;
	release_count = 0;
	DIR *directory = orchfs_opendir("directory");
	require(directory != NULL, "opendir encoded fd zero as NULL");
	require(orchfs_dirfd(directory) == 0,
		"opendir did not decode the first descriptor as zero");
	require(orch_rt.fd_info[0].rw_offset == DIRENT_SIZE * 2,
		"opendir did not initialize the fd-zero directory cursor");
	require(orchfs_closedir(directory) == 0 && release_count == 1 &&
		orch_rt.fd_info[0].used_flag == FD_NOT_USED,
		"closing the fd-zero opendir cursor leaked its descriptor");

	/* With the last slot occupied, a cursor at the end wraps and selects fd 0. */
	reset_file(O_RDONLY, DIR_FILE, DIRENT_SIZE * 3, 0);
	orch_rt.fd_info[0].used_flag = FD_NOT_USED;
	orch_rt.fd_info[ORCH_MAX_FD - 1].used_flag = FD_USED;
	directory_allocation_cursor = ORCH_MAX_FD - 1;
	release_count = 0;
	directory = orchfs_fdopendir(TEST_FILE_FD);
	require(directory != NULL, "fdopendir encoded wrapped fd zero as NULL");
	require(orchfs_dirfd(directory) == 0,
		"fdopendir did not decode the wrapped descriptor as zero");
	require(orchfs_closedir(directory) == 0 && release_count == 1 &&
		orch_rt.fd_info[0].used_flag == FD_NOT_USED,
		"closing the wrapped fd-zero cursor leaked its descriptor");
	orch_rt.fd_info[ORCH_MAX_FD - 1].used_flag = FD_NOT_USED;

	errno = 0;
	require(orchfs_dirfd(NULL) == -1 && errno == EBADF,
		"NULL directory stream decoded as a descriptor");
}

static void test_fd_file_type(void)
{
	reset_file(O_RDONLY, SIMPLE_FILE, 0, 0);
	require(orchfs_fd_file_type(TEST_FILE_FD) == ORCHFS_FILE_TYPE_REGULAR,
		"regular descriptor reported the wrong type");
	reset_file(O_RDONLY, DIR_FILE, DIRENT_SIZE * 2, 0);
	require(orchfs_fd_file_type(TEST_FILE_FD) == ORCHFS_FILE_TYPE_DIRECTORY,
		"directory descriptor reported the wrong type");
	errno = 0;
	require(orchfs_fd_file_type(-1) == ORCHFS_FILE_TYPE_ERROR && errno == EBADF,
		"invalid descriptor type lookup did not return EBADF");
}

static void test_seek_semantics(void)
{
	reset_file(O_RDWR, SIMPLE_FILE, 100, 7);
	require(orchfs_lseek(TEST_FILE_FD, 0, SEEK_END) == 0,
		"SEEK_END(0) failed");
	require(orch_rt.fd_info[TEST_FILE_FD].rw_offset == 100,
		"SEEK_END(0) did not select EOF");
	require(orchfs_lseek(TEST_FILE_FD, 25, SEEK_END) == 0,
		"positive SEEK_END failed");
	require(orch_rt.fd_info[TEST_FILE_FD].rw_offset == 125,
		"positive SEEK_END produced the wrong offset");
	require(orchfs_lseek(TEST_FILE_FD, -20, SEEK_END) == 0,
		"negative SEEK_END failed");
	require(orch_rt.fd_info[TEST_FILE_FD].rw_offset == 80,
		"negative SEEK_END produced the wrong offset");

	errno = 0;
	require(orchfs_lseek(TEST_FILE_FD, -101, SEEK_END) == -1 &&
		errno == EINVAL, "negative resulting offset was accepted");
	require(orch_rt.fd_info[TEST_FILE_FD].rw_offset == 80,
		"failed seek changed the offset");

	orch_rt.fd_info[TEST_FILE_FD].rw_offset = INT64_MAX;
	errno = 0;
	require(orchfs_lseek(TEST_FILE_FD, 1, SEEK_CUR) == -1 &&
		errno == EOVERFLOW, "offset overflow was not rejected");
	require(orch_rt.fd_info[TEST_FILE_FD].rw_offset == INT64_MAX,
		"overflowing seek changed the offset");
}

static void test_ftruncate_validation(void)
{
	reset_file(O_RDONLY, SIMPLE_FILE, 32, 0);
	errno = 0;
	require(orchfs_ftruncate(TEST_FILE_FD, 0) == -1 && errno == EBADF,
		"read-only ftruncate was accepted");
	require(file_inode.i_size == 32, "read-only ftruncate changed size");

	reset_file(O_RDWR, DIR_FILE, 32, 0);
	errno = 0;
	require(orchfs_ftruncate(TEST_FILE_FD, 0) == -1 && errno == EISDIR,
		"directory ftruncate was accepted");
	require(file_inode.i_size == 32, "directory ftruncate changed size");

	reset_file(O_RDWR, SIMPLE_FILE, 32, 0);
	errno = 0;
	require(orchfs_ftruncate(TEST_FILE_FD, SIZE_MAX) == -1 && errno == EFBIG,
		"oversized ftruncate was accepted");

	reset_file(O_RDWR, SIMPLE_FILE, 4, 0);
	require(orchfs_ftruncate(TEST_FILE_FD, 8) == 0,
		"valid extension failed");
	require(file_inode.i_size == 8 && write_count == 1,
		"valid extension did not write zeros and resize");
	require(lock_count == 1 && unlock_count == 1,
		"valid extension did not balance the file lock");
}

static void test_open_exclusive_and_truncate_release(void)
{
	path_exists = 1;
	errno = 0;
	require(orchfs_open("existing", O_CREAT | O_EXCL | O_RDWR, 0644) == -1 &&
		errno == EEXIST, "open O_EXCL accepted an existing path");

	memset(&directory_inode, 0, sizeof(directory_inode));
	directory_inode.i_number = 99;
	directory_inode.i_type = DIR_FILE;
	orch_rt.fd_info[TEST_DIRECTORY_FD].used_flag = FD_USED;
	errno = 0;
	require(orchfs_openat(TEST_DIRECTORY_FD, "existing",
		O_CREAT | O_EXCL | O_RDWR, 0644) == -1 && errno == EEXIST,
		"openat O_EXCL accepted an existing path");

	reset_file(O_RDWR, SIMPLE_FILE, 16, 0);
	path_exists = 1;
	release_count = 0;
	require(orchfs_truncate("existing", 8) == 0,
		"path truncate failed");
	require(file_inode.i_size == 8 && release_count == 1,
		"path truncate leaked its temporary fd");
}

static void reset_rename_test(void)
{
	rename_test_active = 1;
	rename_distinct_parents = 0;
	rename_missing_parent = NULL;
	rename_core_result = 0;
	rename_core_errno = 0;
	rename_core_calls = 0;
	rename_path_calls = 0;
	memset(rename_paths, 0, sizeof(rename_paths));
	memset(renamed_from, 0, sizeof(renamed_from));
	memset(renamed_to, 0, sizeof(renamed_to));
}

static void test_rename_boundary(void)
{
	reset_rename_test();
	require(orchfs_rename("old", "new") == 0,
		"bare relative rename failed");
	require(rename_path_calls == 2 && strcmp(rename_paths[0], ".") == 0 &&
		strcmp(rename_paths[1], ".") == 0,
		"bare rename resolved the wrong parent");
	require(rename_core_calls == 1 && strcmp(renamed_from, "old") == 0 &&
		strcmp(renamed_to, "new") == 0,
		"bare rename parsed the wrong basename");

	reset_rename_test();
	require(orchfs_rename("/old", "/new") == 0,
		"root-level rename failed");
	require(strcmp(rename_paths[0], "/.") == 0 &&
		strcmp(rename_paths[1], "/.") == 0,
		"root rename resolved the wrong parent");

	reset_rename_test();
	rename_missing_parent = "/missing";
	errno = 0;
	require(orchfs_rename("/missing/old", "/missing/new") == -1 &&
		errno == ENOENT && rename_core_calls == 0,
		"missing rename parent was not returned as ENOENT");

	reset_rename_test();
	rename_distinct_parents = 1;
	errno = 0;
	require(orchfs_rename("/left/old", "/right/new") == -1 &&
		errno == EXDEV && rename_core_calls == 0,
		"cross-directory rename was not returned as EXDEV");

	reset_rename_test();
	rename_core_result = -1;
	rename_core_errno = EEXIST;
	errno = 0;
	require(orchfs_rename("old", "new") == -1 && errno == EEXIST,
		"rename collision was not propagated");

	char maximum_name[ORCH_DIRENT_NAME_MAX + 1];
	memset(maximum_name, 'm', ORCH_DIRENT_NAME_MAX);
	maximum_name[ORCH_DIRENT_NAME_MAX] = '\0';
	reset_rename_test();
	require(orchfs_rename("old", maximum_name) == 0 &&
		strlen(renamed_to) == ORCH_DIRENT_NAME_MAX,
		"maximum dirent name was rejected");

	char oversized_name[ORCH_DIRENT_NAME_MAX + 2];
	memset(oversized_name, 'x', ORCH_DIRENT_NAME_MAX + 1);
	oversized_name[ORCH_DIRENT_NAME_MAX + 1] = '\0';
	reset_rename_test();
	errno = 0;
	require(orchfs_rename("old", oversized_name) == -1 &&
		errno == ENAMETOOLONG && rename_core_calls == 0,
		"oversized dirent name reached the legacy core");

	reset_rename_test();
	errno = 0;
	require(orchfs_rename("old", "new/") == -1 && errno == EINVAL,
		"trailing-slash rename was accepted");
	rename_test_active = 0;
}

static void test_shutdown_order(void)
{
	close_event_count = 0;
	init_libfs_server_core();
	close_libfs_server_core();
	const int expected[] = {
		EVENT_STOP_WORKERS,
		EVENT_FREE_LOG,
		EVENT_CLOSE_CACHE,
		EVENT_RETURN_EXTENTS,
		EVENT_FREE_RUNTIME,
		EVENT_FREE_SOCKET,
		EVENT_CLOSE_SHM,
	};
	require(close_event_count == sizeof(expected) / sizeof(expected[0]),
		"unexpected number of close events");
	for(size_t index = 0; index < close_event_count; index++)
		require(close_events[index] == expected[index],
			"workers were not stopped before dependency teardown");
}

int main(void)
{
	test_seek_semantics();
	test_fdopendir_anchors_open_inode();
	test_directory_fd_zero_encoding();
	test_fd_file_type();
	test_ftruncate_validation();
	test_open_exclusive_and_truncate_release();
	test_rename_boundary();
	test_shutdown_order();
	return 0;
}
