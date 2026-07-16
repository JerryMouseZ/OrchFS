#ifndef ORCHFS_ASYNC_ADAPTER_H
#define ORCHFS_ASYNC_ADAPTER_H

#include <dirent.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Blocking compatibility boundary for the LD_PRELOAD wrapper.  File and
 * directory values returned here are adapter-local handles; they never expose
 * KFS handles or coroutine objects to C callers.
 */
int orchfs_async_adapter_init(void);
void orchfs_async_adapter_shutdown(void);
int orchfs_async_adapter_ready(void);
/* Plain relative single-path operations may fall back to libc before the
 * first successful connection, or after a genuine live-session ENOENT. Once
 * a session has existed, transport/storage failures are fail-closed. This
 * policy is deliberately not sufficient for two-path rename: its ENOENT does
 * not identify which operand failed, so the wrapper permits rename fallback
 * only when the adapter has never connected. */
int orchfs_async_adapter_allow_host_fallback(int adapter_was_ready,
                                             int error_number);

int orchfs_async_open(const char *path, int flags, ...);
int orchfs_async_openat(int dirfd, const char *path, int flags, ...);
int orchfs_async_close(int fd);

ssize_t orchfs_async_read(int fd, void *buffer, size_t length);
ssize_t orchfs_async_write(int fd, const void *buffer, size_t length);
ssize_t orchfs_async_pread(int fd, void *buffer, size_t length, off_t offset);
ssize_t orchfs_async_pwrite(int fd, const void *buffer, size_t length,
                            off_t offset);
off_t orchfs_async_lseek(int fd, off_t offset, int whence);

int orchfs_async_fstat(int fd, struct stat *stat_buffer);
int orchfs_async_stat(const char *path, struct stat *stat_buffer);
int orchfs_async_statfs(const char *path, struct statfs *stat_buffer);
int orchfs_async_fstatfs(int fd, struct statfs *stat_buffer);
int orchfs_async_truncate(const char *path, off_t length);
int orchfs_async_ftruncate(int fd, off_t length);
int orchfs_async_fsync(int fd);
int orchfs_async_fcntl(int fd, int command, ...);
int orchfs_async_access(const char *path, int mode);

int orchfs_async_mkdir(const char *path, mode_t mode);
int orchfs_async_rmdir(const char *path);
int orchfs_async_unlink(const char *path);
int orchfs_async_rename(const char *old_path, const char *new_path);

DIR *orchfs_async_opendir(const char *path);
int orchfs_async_is_directory(DIR *directory);
struct dirent *orchfs_async_readdir(DIR *directory);
struct dirent64 *orchfs_async_readdir64(DIR *directory);
int orchfs_async_closedir(DIR *directory);

#ifdef __cplusplus
}
#endif

#endif
