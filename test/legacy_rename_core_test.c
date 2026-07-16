#include "../LibFS/dir_cache.h"
#include "../LibFS/lib_dir.h"
#include "../LibFS/lib_inode.h"
#include "../LibFS/orchfs.h"
#include "../LibFS/runtime.h"

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TEST_DIRECTORY_FD = 12, TEST_DIRECTORY_INODE = 42 };

static orch_inode_t directory_inode;
static orch_dirent_t directory_entries[4];
static int allocated_fds;
static int released_fds;
static int lock_count;
static int unlock_count;
static int write_count;
static int cache_clear_count;

static void require(int condition, const char *message)
{
	if(!condition)
	{
		fprintf(stderr, "legacy_rename_core_test: %s\n", message);
		exit(1);
	}
}

static void set_entry(size_t index, int64_t inode, unsigned char type,
	const char *name)
{
	memset(&directory_entries[index], 0, sizeof(directory_entries[index]));
	directory_entries[index].d_ino = inode;
	directory_entries[index].d_type = type;
	directory_entries[index].d_namelen = (unsigned short)strlen(name);
	strcpy(directory_entries[index].d_name, name);
}

static void reset_directory(void)
{
	memset(&directory_inode, 0, sizeof(directory_inode));
	directory_inode.i_number = TEST_DIRECTORY_INODE;
	directory_inode.i_type = DIR_FILE;
	directory_inode.i_size = sizeof(directory_entries);
	set_entry(0, TEST_DIRECTORY_INODE, DIRENT_FILE_T, ".");
	set_entry(1, TEST_DIRECTORY_INODE, DIRENT_FILE_T, "..");
	set_entry(2, 100, SIMPLE_FILE_T, "old");
	memset(&directory_entries[3], 0, sizeof(directory_entries[3]));
	allocated_fds = 0;
	released_fds = 0;
	lock_count = 0;
	unlock_count = 0;
	write_count = 0;
	cache_clear_count = 0;
}

int64_t get_unused_fd(int64_t inode_id, int mode)
{
	require(inode_id == TEST_DIRECTORY_INODE, "opened the wrong parent inode");
	require(mode == O_RDWR, "opened the parent with the wrong mode");
	allocated_fds++;
	return TEST_DIRECTORY_FD;
}

void release_fd(int fd)
{
	require(fd == TEST_DIRECTORY_FD, "released the wrong parent fd");
	released_fds++;
}

orch_inode_pt fd_to_inodept(int fd)
{
	require(fd == TEST_DIRECTORY_FD, "looked up the wrong parent fd");
	return &directory_inode;
}

void file_lock_wrlock(int fd)
{
	require(fd == TEST_DIRECTORY_FD, "locked the wrong parent fd");
	lock_count++;
}

void file_lock_unlock(int fd)
{
	require(fd == TEST_DIRECTORY_FD, "unlocked the wrong parent fd");
	unlock_count++;
}

void read_from_file(int fd, int64_t offset, int64_t length, void *buffer)
{
	require(fd == TEST_DIRECTORY_FD && offset == 0 &&
		length == (int64_t)sizeof(directory_entries),
		"read the wrong directory range");
	memcpy(buffer, directory_entries, sizeof(directory_entries));
}

void write_into_file(int fd, int64_t offset, int64_t length, void *buffer)
{
	require(fd == TEST_DIRECTORY_FD && length == DIRENT_SIZE &&
		offset >= 0 && offset < (int64_t)sizeof(directory_entries) &&
		offset % DIRENT_SIZE == 0,
		"wrote the wrong directory range");
	memcpy((char *)directory_entries + offset, buffer, DIRENT_SIZE);
	write_count++;
}

void clear_dir_cache(dir_cache_pt cache)
{
	(void)cache;
	cache_clear_count++;
}

static void test_success_and_cache_invalidation(void)
{
	reset_directory();
	errno = 0;
	require(file_rename(TEST_DIRECTORY_INODE, "old", "new") == 0,
		"valid rename failed");
	require(strcmp(directory_entries[2].d_name, "new") == 0 &&
		directory_entries[2].d_ino == 100,
		"rename changed the wrong inode or name");
	require(write_count == 1 && cache_clear_count == 1,
		"rename did not write once and invalidate the cache");
	require(allocated_fds == 1 && released_fds == 1 &&
		lock_count == 1 && unlock_count == 1,
		"rename leaked its fd or lock");
}

static void test_missing_and_existing_destination(void)
{
	reset_directory();
	errno = 0;
	require(file_rename(TEST_DIRECTORY_INODE, "missing", "new") == -1 &&
		errno == ENOENT, "missing source did not return ENOENT");
	require(write_count == 0 && cache_clear_count == 0 &&
		released_fds == 1 && lock_count == unlock_count,
		"missing-source rename mutated or leaked state");

	reset_directory();
	set_entry(3, 200, SIMPLE_FILE_T, "new");
	errno = 0;
	require(file_rename(TEST_DIRECTORY_INODE, "old", "new") == -1 &&
		errno == EEXIST, "existing destination did not return EEXIST");
	require(strcmp(directory_entries[2].d_name, "old") == 0 &&
		directory_entries[2].d_ino == 100 &&
		strcmp(directory_entries[3].d_name, "new") == 0 &&
		directory_entries[3].d_ino == 200,
		"collision changed either inode mapping");
	require(write_count == 0 && cache_clear_count == 0 && released_fds == 1,
		"collision mutated or leaked state");
}

static void test_type_and_name_bounds(void)
{
	reset_directory();
	directory_inode.i_type = SIMPLE_FILE;
	errno = 0;
	require(file_rename(TEST_DIRECTORY_INODE, "old", "new") == -1 &&
		errno == ENOTDIR, "non-directory parent did not return ENOTDIR");
	require(allocated_fds == 1 && released_fds == 1 && lock_count == 0,
		"non-directory parent leaked its fd");

	char oversized_name[ORCH_DIRENT_NAME_MAX + 2];
	memset(oversized_name, 'x', ORCH_DIRENT_NAME_MAX + 1);
	oversized_name[ORCH_DIRENT_NAME_MAX + 1] = '\0';
	reset_directory();
	errno = 0;
	require(file_rename(TEST_DIRECTORY_INODE, "old", oversized_name) == -1 &&
		errno == ENAMETOOLONG, "oversized name was accepted");
	require(allocated_fds == 0 && write_count == 0,
		"oversized name reached directory storage");
}

int main(void)
{
	pthread_rwlock_init(&orch_rt.dir_cache_lock, NULL);
	test_success_and_cache_invalidation();
	test_missing_and_existing_destination();
	test_type_and_name_bounds();
	pthread_rwlock_destroy(&orch_rt.dir_cache_lock);
	return 0;
}
