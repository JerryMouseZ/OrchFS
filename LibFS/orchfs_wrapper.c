#define _GNU_SOURCE
#include <dlfcn.h>
#include <sys/types.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <sys/vfs.h>
#include <dirent.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdatomic.h>
#include <sched.h>

#include "async_adapter.h"
#include <boost/preprocessor/seq/for_each.hpp>
#include "../config/config.h"

/* The preload boundary remains synchronous, but every OrchFS operation now
 * crosses the C++20 client Runtime through this blocking adapter. */
#define orchfs_open orchfs_async_open
#define orchfs_openat orchfs_async_openat
#define orchfs_close orchfs_async_close
#define orchfs_read orchfs_async_read
#define orchfs_write orchfs_async_write
#define orchfs_pread orchfs_async_pread
#define orchfs_pwrite orchfs_async_pwrite
#define orchfs_lseek orchfs_async_lseek
#define orchfs_stat orchfs_async_stat
#define orchfs_fstat orchfs_async_fstat
#define orchfs_statfs orchfs_async_statfs
#define orchfs_fstatfs orchfs_async_fstatfs
#define orchfs_truncate orchfs_async_truncate
#define orchfs_ftruncate orchfs_async_ftruncate
#define orchfs_fsync orchfs_async_fsync
#define orchfs_fcntl orchfs_async_fcntl
#define orchfs_mkdir orchfs_async_mkdir
#define orchfs_rmdir orchfs_async_rmdir
#define orchfs_unlink orchfs_async_unlink
#define orchfs_rename orchfs_async_rename
#define orchfs_opendir orchfs_async_opendir
#define orchfs_readdir orchfs_async_readdir
#define orchfs_closedir orchfs_async_closedir
// #define WRAPPER_DEBUG

// #ifdef __cplusplus       
// extern "C"{
// #endif

#ifdef WRAPPER_DEBUG
#define PRINT_FUNC	printf("\tcalled ctFS func: %s\n", __func__)
#else
#define PRINT_FUNC	;
#endif

#define ORCH_FD_OFFSET 1048576
// #define ORCH_FD_OFFSET 10000000

static int has_orchfs_prefix(const char *path)
{
	return path != NULL && strncmp(path, "/Or", 3) == 0 &&
		(path[3] == '/' || path[3] == '\0');
}

static const char *strip_orchfs_prefix(const char *path)
{
	return path[3] == '\0' ? "/" : path + 3;
}

static int open_needs_mode(int flags)
{
	if((flags & O_CREAT) != 0)
		return 1;
#ifdef O_TMPFILE
	if((flags & O_TMPFILE) == O_TMPFILE)
		return 1;
#endif
	return 0;
}

static int should_fallback_relative_path(int adapter_was_ready)
{
	/* If a live OrchFS session fails with a transport or storage error, silently
	 * retrying in the host namespace can corrupt an unrelated same-named file.
	 * Only a genuine lookup miss, or an adapter that was absent before the call,
	 * is an intentional relative-path fallback. */
	return orchfs_async_adapter_allow_host_fallback(adapter_was_ready, errno);
}

static int should_fallback_openat_relative_path(int adapter_was_ready,
						 int dirfd)
{
	/* A relative path paired with an OrchFS virtual descriptor names only the
	 * OrchFS namespace.  Passing that descriptor to libc would replace the
	 * useful OrchFS error with EBADF (and could cross namespaces if descriptor
	 * encodings ever change).  AT_FDCWD is the sole ambiguous relative case. */
	return dirfd == AT_FDCWD &&
		should_fallback_relative_path(adapter_was_ready);
}

static int should_fallback_mutating_relative_path(int adapter_was_ready)
{
	/* Once a live OrchFS namespace has selected this operation, an ENOENT is
	 * authoritative. Retrying rename in the host cwd could move an unrelated
	 * same-named host file. Preserve host-only operation when the adapter was
	 * unavailable before it had ever connected, but never cross namespaces
	 * after an OrchFS lookup was attempted successfully. */
	return !adapter_was_ready &&
		should_fallback_relative_path(adapter_was_ready);
}

// int64_t all_read_size;

# define EMPTY(...)
# define DEFER(...) __VA_ARGS__ EMPTY()
# define OBSTRUCT(...) __VA_ARGS__ DEFER(EMPTY)()
# define EXPAND(...) __VA_ARGS_

#define MK_STR(arg) #arg
#define MK_STR2(x) MK_STR(x)
#define MK_STR3(x) MK_STR2(x)
// Information about the functions which are wrapped by EVERY module
// Alias: the standard function which most users will call
#define ALIAS_OPEN   open 
#define ALIAS_CREAT  creat 
#define ALIAS_EXECVE execve 
#define ALIAS_EXECVP execvp 
#define ALIAS_EXECV execv 
#define ALIAS_MKNOD __xmknod 
#define ALIAS_MKNODAT __xmknodat 

#define ALIAS_FOPEN  	fopen 
#define ALIAS_FOPEN64  	fopen64 
#define ALIAS_FREAD  	fread 
#define ALIAS_FEOF 	 	feof 
#define ALIAS_FERROR 	ferror 
#define ALIAS_CLEARERR 	clearerr 
#define ALIAS_FWRITE 	fwrite 
#define ALIAS_FSEEK  	fseek 
#define ALIAS_FTELL  	ftell 
#define ALIAS_FTELLO 	ftello 
#define ALIAS_FCLOSE 	fclose 
#define ALIAS_FPUTS		fputs 
#define ALIAS_FGETS		fgets 
#define ALIAS_FFLUSH	fflush 

#define ALIAS_FSTATFS	fstatfs 
#define ALIAS_STATFS	statfs
#define ALIAS_FDATASYNC	fdatasync 
#define ALIAS_FCNTL		fcntl 
#define ALIAS_FCNTL2	__fcntl64_nocancel_adjusted 
#define ALIAS_OPENDIR		opendir 
#define ALIAS_READDIR	readdir 
#define ALIAS_READDIR64	readdir64 
#define ALIAS_CLOSEDIR	closedir 
#define ALIAS_ERROR		__errno_location 
#define ALIAS_SYNC_FILE_RANGE	sync_file_range 

#define ALIAS_ACCESS access 
#define ALIAS_READ   read 
#define ALIAS_READ2		__libc_read 
#define ALIAS_WRITE  write 
#define ALIAS_SEEK   lseek 
#define ALIAS_CLOSE  close 
#define ALIAS_FTRUNC ftruncate 
#define ALIAS_TRUNC  truncate 
#define ALIAS_DUP    dup 
#define ALIAS_DUP2   dup2 
#define ALIAS_FORK   fork 
#define ALIAS_VFORK  vfork 
#define ALIAS_MMAP   mmap 
#define ALIAS_READV  readv 
#define ALIAS_WRITEV writev 
#define ALIAS_PIPE   pipe 
#define ALIAS_SOCKETPAIR   socketpair 
#define ALIAS_IOCTL  ioctl 
#define ALIAS_MUNMAP munmap 
#define ALIAS_MSYNC  msync 
#define ALIAS_CLONE  __clone 
#define ALIAS_PREAD  pread 
#define ALIAS_PREAD64  pread64 
#define ALIAS_PWRITE64 pwrite64 
#define ALIAS_PWRITE pwrite 
//#define ALIAS_PWRITESYNC pwrite64_sync
#define ALIAS_FSYNC  fsync 
#define ALIAS_FDSYNC fdatasync 
#define ALIAS_FTRUNC64 ftruncate64 
#define ALIAS_OPEN64  open64 
#define ALIAS_LIBC_OPEN64 __libc_open64 
#define ALIAS_SEEK64  lseek64 
#define ALIAS_MMAP64  mmap64 
#define ALIAS_MKSTEMP mkstemp 
#define ALIAS_MKSTEMP64 mkstemp64 
#define ALIAS_ACCEPT  accept 
#define ALIAS_SOCKET  socket 
#define ALIAS_UNLINK  unlink 
#define ALIAS_POSIX_FALLOCATE posix_fallocate 
#define ALIAS_POSIX_FALLOCATE64 posix_fallocate64 
#define ALIAS_FALLOCATE fallocate 

#ifdef _STAT_VER
#define ALIAS_XSTAT __xstat
#else 
#define ALIAS_STAT stat 
#endif
#ifdef _STAT_VER
#define ALIAS_FXSTAT __fxstat
#else 
#define ALIAS_FSTAT fstat
#endif

#define ALIAS_STAT64 stat64 
#define ALIAS_FSTAT64 fstat64 
#define ALIAS_LSTAT lstat 
#define ALIAS_LSTAT64 lstat64 
/* Now all the metadata operations */
#define ALIAS_MKDIR mkdir 
#define ALIAS_RENAME rename 
#define ALIAS_LINK link 
#define ALIAS_SYMLINK symlink 
#define ALIAS_RMDIR rmdir 
/* All the *at operations */
#define ALIAS_OPENAT openat 
#define ALIAS_SYMLINKAT symlinkat 
#define ALIAS_MKDIRAT mkdirat 
#define ALIAS_UNLINKAT  unlinkat 


// The function return type
#define RETT_OPEN   int
#define RETT_LIBC_OPEN64 int
#define RETT_CREAT  int
#define RETT_EXECVE int
#define RETT_EXECVP int
#define RETT_EXECV int
#define RETT_SHM_COPY void
#define RETT_MKNOD int
#define RETT_MKNODAT int

// #ifdef TRACE_FP_CALLS
#define RETT_FOPEN  FILE*
#define RETT_FOPEN64  FILE*
#define RETT_FWRITE size_t
#define RETT_FSEEK  int
#define RETT_FTELL  long int
#define RETT_FTELLO off_t
#define RETT_FCLOSE int
#define RETT_FPUTS	int
#define RETT_FGETS	char*
#define RETT_FFLUSH	int
// #endif

#define RETT_FSTATFS	int
#define RETT_STATFS	int
#define RETT_FDATASYNC	int
#define RETT_FCNTL		int
#define RETT_FCNTL2		int
#define RETT_OPENDIR	DIR *
#define RETT_READDIR	struct dirent *
#define RETT_READDIR64	struct dirent64 *
#define RETT_CLOSEDIR	int
#define RETT_ERROR		int *
#define RETT_SYNC_FILE_RANGE int

#define RETT_ACCESS int
#define RETT_READ   ssize_t
#define RETT_READ2   ssize_t
#define RETT_FREAD  size_t
#define RETT_FEOF   int
#define RETT_FERROR int
#define RETT_CLEARERR void
#define RETT_WRITE  ssize_t
#define RETT_SEEK   off_t
#define RETT_CLOSE  int
#define RETT_FTRUNC int
#define RETT_TRUNC  int
#define RETT_DUP    int
#define RETT_DUP2   int
#define RETT_FORK   pid_t
#define RETT_VFORK  pid_t
#define RETT_MMAP   void*
#define RETT_READV  ssize_t
#define RETT_WRITEV ssize_t
#define RETT_PIPE   int
#define RETT_SOCKETPAIR   int
#define RETT_IOCTL  int
#define RETT_MUNMAP int
#define RETT_MSYNC  int
#define RETT_CLONE  int
#define RETT_PREAD  ssize_t
#define RETT_PREAD64  ssize_t
#define RETT_PWRITE ssize_t
#define RETT_PWRITE64 ssize_t
//#define RETT_PWRITESYNC ssize_t
#define RETT_FSYNC  int
#define RETT_FDSYNC int
#define RETT_FTRUNC64 int
#define RETT_OPEN64  int
#define RETT_SEEK64  off64_t
#define RETT_MMAP64  void*
#define RETT_MKSTEMP int
#define RETT_MKSTEMP64 int
#define RETT_ACCEPT  int
#define RETT_SOCKET  int
#define RETT_UNLINK  int
#define RETT_POSIX_FALLOCATE int
#define RETT_POSIX_FALLOCATE64 int
#define RETT_FALLOCATE int

#ifdef _STAT_VER
#define RETT_XSTAT int
#else 
#define RETT_STAT int
#endif
#ifdef _STAT_VER
#define RETT_FXSTAT int
#else 
#define RETT_FSTAT int
#endif

#define RETT_STAT64 int
#define RETT_FSTAT64 int
#define RETT_LSTAT int
#define RETT_LSTAT64 int
/* Now all the metadata operations */
#define RETT_MKDIR int
#define RETT_RENAME int
#define RETT_LINK int
#define RETT_SYMLINK int
#define RETT_RMDIR int
/* All the *at operations */
#define RETT_OPENAT int
#define RETT_SYMLINKAT int
#define RETT_MKDIRAT int
#define RETT_UNLINKAT int


// The function interface
#define INTF_OPEN const char *path, int oflag, ...
#define INTF_LIBC_OPEN64 const char *path, int oflag, ...

#define INTF_CREAT const char *path, mode_t mode
#define INTF_EXECVE const char *filename, char *const argv[], char *const envp[]
#define INTF_EXECVP const char *file, char *const argv[]
#define INTF_EXECV const char *path, char *const argv[]
#define INTF_SHM_COPY void
#define INTF_MKNOD int ver, const char* path, mode_t mode, dev_t* dev
#define INTF_MKNODAT int ver, int dirfd, const char* path, mode_t mode, dev_t* dev

// #ifdef TRACE_FP_CALLS
#define INTF_FOPEN  const char* __restrict path, const char* __restrict mode
#define INTF_FOPEN64  const char* __restrict path, const char* __restrict mode
#define INTF_FREAD  void* __restrict buf, size_t length, size_t nmemb, FILE* __restrict fp
#define INTF_CLEARERR FILE* fp
#define INTF_FEOF   FILE* fp
#define INTF_FERROR FILE* fp
#define INTF_FWRITE const void* __restrict buf, size_t length, size_t nmemb, FILE* __restrict fp
#define INTF_FSEEK  FILE* fp, long int offset, int whence
#define INTF_FTELL  FILE* fp
#define INTF_FTELLO FILE* fp
#define INTF_FCLOSE FILE* fp
#define INTF_FPUTS	const char *str, FILE *stream
#define INTF_FGETS	char *str, int n, FILE *stream
#define INTF_FFLUSH	FILE* fp
// #endif

#define INTF_FSTATFS	int fd, struct statfs *buf
#define INTF_STATFS	const char *path, struct statfs *buf
#define INTF_FDATASYNC	int fd
#define INTF_FCNTL		int fd, int cmd, ...
#define INTF_FCNTL2		int fd, int cmd, void *arg
#define INTF_OPENDIR	const char *path
#define INTF_READDIR	DIR *dirp
#define INTF_READDIR64	DIR *dirp
#define INTF_CLOSEDIR	DIR *dirp
#define INTF_ERROR		void
#define INTF_SYNC_FILE_RANGE int fd, off64_t offset, off64_t nbytes, unsigned int flags


#define INTF_ACCESS const char *pathname, int mode
#define INTF_READ   int file, void* buf, size_t length
#define INTF_READ2   int file, void* buf, size_t length
#define INTF_WRITE  int file, const void* buf, size_t length
#define INTF_SEEK   int file, off_t offset, int whence
#define INTF_CLOSE  int file
#define INTF_FTRUNC int file, off_t length
#define INTF_TRUNC  const char* path, off_t length
#define INTF_DUP    int file
#define INTF_DUP2   int file, int fd2
#define INTF_FORK   void
#define INTF_VFORK  void
#define INTF_MMAP   void *addr, size_t len, int prot, int flags, int file, off_t off
#define INTF_READV  int file, const struct iovec *iov, int iovcnt
#define INTF_WRITEV int file, const struct iovec *iov, int iovcnt
#define INTF_PIPE   int file[2]
#define INTF_SOCKETPAIR   int domain, int type, int protocol, int sv[2]
#define INTF_IOCTL  int file, unsigned long int request, ...
#define INTF_MUNMAP void *addr, size_t len
#define INTF_MSYNC  void *addr, size_t len, int flags
#define INTF_CLONE  int (*fn)(void *a), void *child_stack, int flags, void *arg
#define INTF_PREAD  int file,       void *buf, size_t count, off_t offset
#define INTF_PREAD64  int file,       void *buf, size_t count, off_t offset
#define INTF_PWRITE int file, const void *buf, size_t count, off_t offset
#define INTF_PWRITE64 int file, const void *buf, size_t count, off_t offset
//#define INTF_PWRITESYNC int file, const void *buf, size_t count, off_t offset
#define INTF_FSYNC  int file
#define INTF_FDSYNC int file
#define INTF_FTRUNC64 int file, off64_t length
#define INTF_OPEN64  const char* path, int oflag, ...
#define INTF_SEEK64  int file, off64_t offset, int whence
#define INTF_MMAP64  void *addr, size_t len, int prot, int flags, int file, off64_t off
#define INTF_MKSTEMP char* file
#define INTF_MKSTEMP64 char* file
#define INTF_ACCEPT  int file, struct sockaddr *addr, socklen_t *addrlen
#define INTF_SOCKET  int domain, int type, int protocol
#define INTF_UNLINK  const char* path
#define INTF_POSIX_FALLOCATE int file, off_t offset, off_t len
#define INTF_POSIX_FALLOCATE64 int file, off_t offset, off_t len
#define INTF_FALLOCATE int file, int mode, off_t offset, off_t len

#ifdef _STAT_VER
#define INTF_XSTAT int ver,const char *path, struct stat *buf
#else 
#define INTF_STAT const char *path, struct stat *buf
#endif
#ifdef _STAT_VER
#define INTF_FXSTAT int ver, int file, struct stat *buf
#else 
#define INTF_FSTAT int file, struct stat *buf
#endif


#define INTF_STAT64 const char *path, struct stat64 *buf
#define INTF_FSTAT64 int file, struct stat64 *buf
#define INTF_LSTAT const char *path, struct stat *buf
#define INTF_LSTAT64 const char *path, struct stat64 *buf
/* Now all the metadata operations */
#define INTF_MKDIR const char *path, uint32_t mode
#define INTF_RENAME const char *old, const char *new
#define INTF_LINK const char *path1, const char *path2
#define INTF_SYMLINK const char *path1, const char *path2
#define INTF_RMDIR const char *path
/* All the *at operations */
#define INTF_OPENAT int dirfd, const char* path, int oflag, ...
#define INTF_UNLINKAT  int dirfd, const char* path, int flags
#define INTF_SYMLINKAT const char* old_path, int newdirfd, const char* new_path
#define INTF_MKDIRAT int dirfd, const char* path, mode_t mode





// The function interface
#define M_INTF_OPEN const char *path, int oflag, ...
#define M_INTF_LIBC_OPEN64 const char *path, int oflag, ...

#define M_INTF_CREAT const char *path, mode_t mode
#define M_INTF_EXECVE const char *filename, char *const argv[], char *const envp[]
#define M_INTF_EXECVP const char *file, char *const argv[]
#define M_INTF_EXECV const char *path, char *const argv[]
#define M_INTF_SHM_COPY void
#define M_INTF_MKNOD int ver, const char* path, mode_t mode, dev_t* dev
#define M_INTF_MKNODAT int ver, int dirfd, const char* path, mode_t mode, dev_t* dev

// #ifdef TRACE_FP_CALLS
#define M_INTF_FOPEN  const char* __restrict path, const char* __restrict mode
#define M_INTF_FOPEN64  const char* __restrict path, const char* __restrict mode
#define M_INTF_FREAD  void* __restrict buf, size_t length, size_t nmemb, FILE* __restrict fp, int op
#define M_INTF_CLEARERR FILE* fp
#define M_INTF_FEOF   FILE* fp
#define M_INTF_FERROR FILE* fp
#define M_INTF_FWRITE const void* __restrict buf, size_t length, size_t nmemb, FILE* __restrict fp, int op
#define M_INTF_FSEEK  FILE* fp, long int offset, int whence, int op
#define M_INTF_FTELL  FILE* fp
#define M_INTF_FTELLO FILE* fp
#define M_INTF_FCLOSE FILE* fp
#define M_INTF_FPUTS	const char *str, FILE *stream, int op
#define M_INTF_FGETS	char *str, int n, FILE *stream, int op
#define M_INTF_FFLUSH	FILE* fp
// #endif

#define M_INTF_FSTATFS	int fd, struct statfs *buf
#define M_INTF_STATFS	const char *path, struct statfs *buf
#define M_INTF_FDATASYNC	int fd
#define M_INTF_FCNTL		int fd, int cmd, ...
#define M_INTF_FCNTL2		int fd, int cmd, void *arg
#define M_INTF_OPENDIR	const char *path
#define M_INTF_READDIR	DIR *dirp
#define M_INTF_READDIR64	DIR *dirp
#define M_INTF_CLOSEDIR	DIR *dirp
#define M_INTF_ERROR		void
#define M_INTF_SYNC_FILE_RANGE int fd, off64_t offset, off64_t nbytes, unsigned int flags

#define M_INTF_ACCESS const char *pathname, int mode
#define M_INTF_READ   int file, void* buf, size_t length, int op
#define M_INTF_READ2   int file, void* buf, size_t length, int op
#define M_INTF_WRITE  int file, const void* buf, size_t length,int op
#define M_INTF_SEEK   int file, size_t offset, int whence, int op
#define M_INTF_CLOSE  int file
#define M_INTF_FTRUNC int file, size_t length, int op
#define M_INTF_TRUNC  const char* path, size_t length, int op
#define M_INTF_DUP    int file
#define M_INTF_DUP2   int file, int fd2
#define M_INTF_FORK   void
#define M_INTF_VFORK  void
#define M_INTF_MMAP   void *addr, size_t len, int prot, int flags, int file, off_t off
#define M_INTF_READV  int file, const struct iovec *iov, int iovcnt
#define M_INTF_WRITEV int file, const struct iovec *iov, int iovcnt
#define M_INTF_PIPE   int file[2]
#define M_INTF_SOCKETPAIR   int domain, int type, int protocol, int sv[2]
#define M_INTF_IOCTL  int file, unsigned long int request, ...
#define M_INTF_MUNMAP void *addr, size_t len
#define M_INTF_MSYNC  void *addr, size_t len, int flags
#define M_INTF_CLONE  int (*fn)(void *a), void *child_stack, int flags, void *arg
#define M_INTF_PREAD  int file,       void *buf, size_t count, size_t offset, int op
#define M_INTF_PREAD64  int file,       void *buf, size_t count, size_t offset, int op
#define M_INTF_PWRITE int file, const void *buf, size_t count, size_t offset, int op
#define M_INTF_PWRITE64 int file, const void *buf, size_t count, size_t offset, int op
//#define M_INTF_PWRITESYNC int file, const void *buf, size_t count, off_t offset
#define M_INTF_FSYNC  int file
#define M_INTF_FDSYNC int file
#define M_INTF_FTRUNC64 int file, off64_t length, int op
#define M_INTF_OPEN64  const char* path, int oflag, ...
#define M_INTF_SEEK64  int file, off64_t offset, int whence, int op
#define M_INTF_MMAP64  void *addr, size_t len, int prot, int flags, int file, off64_t off
#define M_INTF_MKSTEMP char* file
#define M_INTF_MKSTEMP64 char* file
#define M_INTF_ACCEPT  int file, struct sockaddr *addr, socklen_t *addrlen
#define M_INTF_SOCKET  int domain, int type, int protocol
#define M_INTF_UNLINK  const char* path
#define M_INTF_POSIX_FALLOCATE int file, off_t offset, off_t len
#define M_INTF_POSIX_FALLOCATE64 int file, off_t offset, off_t len
#define M_INTF_FALLOCATE int file, int mode, off_t offset, off_t len
#define M_INTF_STAT const char *path, struct stat *buf
#define M_INTF_STAT64 const char *path, struct stat64 *buf
#define M_INTF_FSTAT int file, struct stat *buf
#define M_INTF_FSTAT64 int file, struct stat64 *buf
#define M_INTF_LSTAT const char *path, struct stat *buf
#define M_INTF_LSTAT64 const char *path, struct stat64 *buf
/* Now all the metadata operations */
#define M_INTF_MKDIR const char *path, uint32_t mode
#define M_INTF_RENAME const char *old, const char *new
#define M_INTF_LINK const char *path1, const char *path2
#define M_INTF_SYMLINK const char *path1, const char *path2
#define M_INTF_RMDIR const char *path
/* All the *at operations */
#define M_INTF_OPENAT int dirfd, const char* path, int oflag, ...
#define M_INTF_UNLINKAT  int dirfd, const char* path, int flags
#define M_INTF_SYMLINKAT const char* old_path, int newdirfd, const char* new_path
#define M_INTF_MKDIRAT int dirfd, const char* path, mode_t mode





#ifdef _STAT_VER
#define ORCHFS_ALL_OPS	(OPEN) (OPEN64) (LIBC_OPEN64) (OPENAT) (CREAT) (CLOSE) (ACCESS) \
						(SEEK) (SEEK64) (TRUNC) (FTRUNC) (FTRUNC64) (LINK) (UNLINK) (FSYNC) \
						(READ) (READ2) (WRITE) (PREAD) (PREAD64) (PWRITE) (PWRITE64) (XSTAT) (STAT64) (FXSTAT) (FSTAT64) (LSTAT) (RENAME)\
						(MKDIR) (RMDIR) (STATFS) (FSTATFS) (FDATASYNC) (FCNTL) (FCNTL2) \
						(OPENDIR) (CLOSEDIR) (READDIR) (READDIR64) (SYNC_FILE_RANGE) \
							(FOPEN) (FOPEN64) (FPUTS) (FGETS) (FWRITE) (FREAD) (FCLOSE) (FSEEK) (FFLUSH)
#else 
#define ORCHFS_ALL_OPS	(OPEN) (OPEN64) (LIBC_OPEN64) (OPENAT) (CREAT) (CLOSE) (ACCESS) \
						(SEEK) (SEEK64) (TRUNC) (FTRUNC) (FTRUNC64) (LINK) (UNLINK) (FSYNC) \
						(READ) (READ2) (WRITE) (PREAD) (PREAD64) (PWRITE) (PWRITE64) (STAT) (STAT64) (FSTAT) (FSTAT64) (LSTAT) (RENAME)\
						(MKDIR) (RMDIR) (STATFS) (FSTATFS) (FDATASYNC) (FCNTL) (FCNTL2) \
						(OPENDIR) (CLOSEDIR) (READDIR) (READDIR64) (SYNC_FILE_RANGE) \
							(FOPEN) (FOPEN64) (FPUTS) (FGETS) (FWRITE) (FREAD) (FCLOSE) (FSEEK) (FFLUSH)
#endif



#define PREFIX(call)				(real_##call)


#define TYPE_REL_SYSCALL(op) 		typedef RETT_##op (*real_##op##_t)(INTF_##op);
#define TYPE_REL_SYSCALL_WRAP(r, data, elem) 		TYPE_REL_SYSCALL(elem)

BOOST_PP_SEQ_FOR_EACH(TYPE_REL_SYSCALL_WRAP, placeholder, ORCHFS_ALL_OPS)

static struct real_ops{
	#define DEC_REL_SYSCALL(op) 	real_##op##_t op;
	#define DEC_REL_SYSCALL_WRAP(r, data, elem) 	DEC_REL_SYSCALL(elem)
	BOOST_PP_SEQ_FOR_EACH(DEC_REL_SYSCALL_WRAP, placeholder, ORCHFS_ALL_OPS)
} real_ops_storage;

static atomic_int real_ops_state = ATOMIC_VAR_INIT(0);
static _Thread_local int resolving_real_ops;

static void insert_real_op(void)
{
	int state = atomic_load_explicit(&real_ops_state, memory_order_acquire);
	if(state == 2)
		return;

	int expected = 0;
	if(!atomic_compare_exchange_strong_explicit(
			&real_ops_state, &expected, 1,
			memory_order_acq_rel, memory_order_acquire)){
		/* dlsym is not expected to re-enter these wrappers, but returning the
		 * partially populated table avoids a same-thread deadlock if a loader
		 * implementation does so. Other threads wait until publication. */
		if(resolving_real_ops)
			return;
		while(atomic_load_explicit(&real_ops_state,
									  memory_order_acquire) != 2)
			sched_yield();
		return;
	}

	resolving_real_ops = 1;
	#define FILL_REL_SYSCALL(op) do { \
		void *symbol = dlsym(RTLD_NEXT, MK_STR3(ALIAS_##op)); \
		_Static_assert(sizeof(real_ops_storage.op) == sizeof(symbol), \
					   "POSIX function pointer size mismatch"); \
		memcpy(&real_ops_storage.op, &symbol, sizeof(symbol)); \
	} while(0);
	#define FILL_REL_SYSCALL_WRAP(r, data, elem) 	FILL_REL_SYSCALL(elem)
	BOOST_PP_SEQ_FOR_EACH(FILL_REL_SYSCALL_WRAP, placeholder, ORCHFS_ALL_OPS)
	/* These glibc-private entry points are absent on some releases. Their
	 * public equivalents have the same contract used by the wrappers. */
	if(real_ops_storage.LIBC_OPEN64 == NULL)
		real_ops_storage.LIBC_OPEN64 = real_ops_storage.OPEN64;
	if(real_ops_storage.READ2 == NULL)
		real_ops_storage.READ2 = real_ops_storage.READ;
	if(real_ops_storage.FCNTL2 == NULL){
		_Static_assert(sizeof(real_ops_storage.FCNTL2) ==
					   sizeof(real_ops_storage.FCNTL),
					   "POSIX function pointer size mismatch");
		memcpy(&real_ops_storage.FCNTL2, &real_ops_storage.FCNTL,
			   sizeof(real_ops_storage.FCNTL2));
	}
	resolving_real_ops = 0;
	atomic_store_explicit(&real_ops_state, 2, memory_order_release);
}

static struct real_ops *get_real_ops(void)
{
	insert_real_op();
	return &real_ops_storage;
}

#define real_ops (*get_real_ops())
#define OP_DEFINE(op)		RETT_##op ALIAS_##op(INTF_##op)

static int inited = 0;
OP_DEFINE(OPEN){
	// printf("open call! %s %d\n", path, oflag & O_CREAT);
	// fflush(stdout);
	if(real_ops.OPEN == NULL){
		insert_real_op();
	}
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	// fprintf(stderr,"open call\n");
	// fprintf(stderr,"open %s\n",path);
	// fprintf(stderr,"oflag=%x\n",oflag);
	if(*path == '\\' || *path != '/' || path[1] == '\\' || dirflag){
		// printf("myopen call! %s %d\n", path, oflag & O_CREAT);
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(open_needs_mode(oflag)){
			// fprintf(stderr,"open 111111\n");
			va_list ap;
			mode_t mode;
			va_start(ap, oflag);
			mode = va_arg(ap, mode_t);
			//printf("222222\n");
			PRINT_FUNC;

			const char * p = path;
			while(*p == '\\'){
				p ++;
			}
			if(dirflag)
					p=strip_orchfs_prefix(path);
			ret = orchfs_open(p, oflag, mode);
			va_end(ap);
			// fprintf(stderr,"O_CREAT open ret=%d\n",ret+1024);
			if(ret == -1 && *path != '\\' && !dirflag &&
			   should_fallback_relative_path(adapter_was_ready)){
				return real_ops.OPEN(path, oflag, mode);
			}
		}
		else{
			PRINT_FUNC;
			if(*path=='\\')
				ret = orchfs_open(path + 1, oflag);
			else if(dirflag)
					ret = orchfs_open(strip_orchfs_prefix(path), oflag);
			else ret=orchfs_open(path,oflag);
			// printf("open ret=%d\n",ret);
			if(ret == -1 && *path != '\\' && !dirflag &&
			   should_fallback_relative_path(adapter_was_ready)){

				return real_ops.OPEN(path, oflag);
			}
		}
		if(ret == -1){
			return -1;
		}
		return ret + ORCH_FD_OFFSET;
	}
	else{
		// fprintf(stderr,"real open:%s\n",path);
		if(open_needs_mode(oflag)){
			va_list ap;
				mode_t mode;
				va_start(ap, oflag);
				mode = va_arg(ap, mode_t);
				va_end(ap);
				return real_ops.OPEN(path, oflag, mode);
		}
		else{
			int ret_fd = real_ops.OPEN(path, oflag);
			// printf("ret_fd: %d\n",ret_fd);
			// fflush(stdout);
			return ret_fd;
		}
	}
}

OP_DEFINE(OPEN64){
	if(open_needs_mode(oflag)){
		va_list ap;
		va_start(ap, oflag);
		mode_t mode = va_arg(ap, mode_t);
		va_end(ap);
		return open(path, oflag, mode);
	}
	return open(path, oflag);
}


OP_DEFINE(LIBC_OPEN64){
	if(open_needs_mode(oflag)){
		va_list ap;
		va_start(ap, oflag);
		mode_t mode = va_arg(ap, mode_t);
		va_end(ap);
		return open(path, oflag, mode);
	}
	return open(path, oflag);
	// printf("libc_open %s\n",path);
// 	int dirflag=0;
// 	if(path[0]=='/'&&path[1]='O'&&path[2]=='r')
// 		dirflag=1;
// 	if(*path == '\\' || *path != '/' || dirflag){
// 		//printf("my lic_open\n");
// 		int ret;
// 		if(oflag & O_CREAT){
// 			va_list ap;
// 			mode_t mode;
// 			va_start(ap, oflag);
// 			mode = va_arg(ap, mode_t);
// 			PRINT_FUNC;
// #ifdef WRAPPER_DEBUG
// 			printf("\t\tpath: %s\n", path);
// #endif
// 			if(*path=='\\')
// 				ret = orchfs_open(path + 1, oflag, mode);
// 			else if(dirflag)
// 				ret = orchfs_open(path + 3, oflag, mode);
// 			else ret = orchfs_open(path, oflag, mode);
// 			if(ret == -1 && *path != '\\' && !dirflag){
// 				return real_ops.OPEN(path, oflag, mode);
// 			}
// 		}
// 		else{
// #ifdef WRAPPER_DEBUG
// 			printf("\t\tpath: %s\n", path);
// #endif
// 			PRINT_FUNC;
// 			if(*path=='\\')
// 				ret = orchfs_open(path + 1, oflag);
// 			else if(dirflag)
// 				ret = orchfs_open(path + 3, oflag);
// 			else ret = orchfs_open(path, oflag);
// 			if(ret == -1 && *path != '\\' && !dirflag){
// 				return real_ops.OPEN(path, oflag);
// 			}
// 		}
// 		if(ret == -1){
// 			return ret;
// 		}
// #ifdef WRAPPER_DEBUG
// 		printf("open returned: %d\n", ret + ORCH_FD_OFFSET);
// #endif
// 		return ret + ORCH_FD_OFFSET;
// 	}
// 	else{
// 		if(oflag & O_CREAT){
// 			va_list ap;
// 			mode_t mode;
// 			va_start(ap, oflag);
// 			mode = va_arg(ap, mode_t);
// 			return real_ops.OPEN(path, oflag, mode);
// 		}
// 		else{
// 			return real_ops.OPEN(path, oflag);
// 		}
// 	}
}

OP_DEFINE(OPENAT){
	// printf("openat %s\n",path);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path != '/' && *path != '\\' && dirfd != AT_FDCWD &&
	   dirfd < ORCH_FD_OFFSET){
		if(open_needs_mode(oflag)){
			va_list ap;
			va_start(ap, oflag);
			mode_t mode = va_arg(ap, mode_t);
			va_end(ap);
			return real_ops.OPENAT(dirfd, path, oflag, mode);
		}
		return real_ops.OPENAT(dirfd, path, oflag);
	}
	if(*path == '\\' || *path != '/' || dirflag){
		//printf("my openat\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		int adapter_dirfd = dirfd >= ORCH_FD_OFFSET ?
							dirfd - ORCH_FD_OFFSET : dirfd;
		if(open_needs_mode(oflag)){
			va_list ap;
			mode_t mode;
			va_start(ap, oflag);
			mode = va_arg(ap, mode_t);
			PRINT_FUNC;
			if(*path=='\\')
				ret = orchfs_openat(adapter_dirfd,path + 1, oflag, mode);
			else if(dirflag)
					ret = orchfs_openat(adapter_dirfd,strip_orchfs_prefix(path), oflag, mode);
			else ret = orchfs_openat(adapter_dirfd,path, oflag, mode);
			va_end(ap);
			//ret = orchfs_openat(dirfd - ORCH_FD_OFFSET, (*path == '\\')? path + 1 : path, oflag, mode);
			if(ret == -1 && *path != '\\' && !dirflag &&
			   should_fallback_openat_relative_path(adapter_was_ready,
									 dirfd)){
				return real_ops.OPENAT(dirfd, path, oflag, mode);
			}
		}
		else{
			PRINT_FUNC;
			if(*path=='\\')
				ret = orchfs_openat(adapter_dirfd,path + 1, oflag);
			else if(dirflag)
					ret = orchfs_openat(adapter_dirfd,strip_orchfs_prefix(path), oflag);
			else ret = orchfs_openat(adapter_dirfd,path, oflag);
			//ret = orchfs_openat(dirfd - ORCH_FD_OFFSET, (*path == '\\')? path + 1 : path, oflag);
			if(ret == -1 && *path != '\\' && !dirflag &&
			   should_fallback_openat_relative_path(adapter_was_ready,
									 dirfd)){
				return real_ops.OPENAT(dirfd, path, oflag);
			}
		}
		if(ret == -1){
			return ret;
		}
		return ret + ORCH_FD_OFFSET;
	}
	else{
		if(open_needs_mode(oflag)){
			va_list ap;
				mode_t mode;
				va_start(ap, oflag);
				mode = va_arg(ap, mode_t);
				va_end(ap);
				return real_ops.OPENAT(dirfd, path, oflag, mode);
		}
		else{
			return real_ops.OPENAT(dirfd, path, oflag);
		}
	}
}

OP_DEFINE(CREAT){
	// printf("creat call\n");
	// fflush(stdout);
	// printf("creat path=%s\n",path);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		// printf("my creat\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_open(path + 1,O_WRONLY | O_CREAT | O_TRUNC, mode);
		else if(dirflag)
			ret = orchfs_open(strip_orchfs_prefix(path),O_WRONLY | O_CREAT | O_TRUNC, mode);
		else 
			ret = orchfs_open(path, O_WRONLY | O_CREAT | O_TRUNC, mode);
		//int ret = orchfs_open((*path == '\\')? path + 1 : path, O_CREAT, mode);
		if(ret == -1 && *path != '\\' && !dirflag &&
		   should_fallback_relative_path(adapter_was_ready)){
			return real_ops.CREAT(path, mode);
		}
		return ret == -1 ? -1 : ret + ORCH_FD_OFFSET;
	}
	else{
		return real_ops.CREAT(path, mode);
	}
}

OP_DEFINE(CLOSE){
	// printf("close call\n");
	// printf("clse fd=%d\n",file);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		// fprintf(stderr,"my close\n");
		return orchfs_close(file - ORCH_FD_OFFSET);
	}
	else{
		return real_ops.CLOSE(file);
	}
}

OP_DEFINE(ACCESS){
	// printf("access call\n");
	// printf("access %s\n",pathname);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(pathname))
		dirflag=1;
	if(*pathname == '\\' || *pathname != '/' ||dirflag){
		// PRINT_FUNC;
		// printf("my access %d\n",dirflag);
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*pathname=='\\')
			ret = orchfs_async_access(pathname + 1, mode);
		else if(dirflag)
			ret = orchfs_async_access(strip_orchfs_prefix(pathname), mode);
		else
			ret = orchfs_async_access(pathname, mode);
		if(ret == -1 && *pathname != '\\' && !dirflag &&
		   should_fallback_relative_path(adapter_was_ready)){
			return real_ops.ACCESS(pathname, mode);
		}
		return ret;
	}
	else{
		return real_ops.ACCESS(pathname, mode);
	}
}

OP_DEFINE(SEEK){
	// printf("seek call, fd=%d\n",file);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET)
	{
		PRINT_FUNC;
		return orchfs_lseek(file - ORCH_FD_OFFSET, offset, whence);
	}
	else
	{
		return real_ops.SEEK(file, offset, whence);
	}
}

OP_DEFINE(SEEK64){
	if(file >= ORCH_FD_OFFSET)
		return (off64_t)orchfs_lseek(file - ORCH_FD_OFFSET,
									 (off_t)offset, whence);
	return real_ops.SEEK64(file, offset, whence);
}

OP_DEFINE(TRUNC){
	// printf("trunc call\n");
	// fflush(stdout);
	// printf("trunc path=%s length=%d\n",path,length);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my trunc\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_truncate(path + 1, length);
		else if(dirflag)
				ret = orchfs_truncate(strip_orchfs_prefix(path), length);
		else ret = orchfs_truncate(path, length);
		if(ret == -1){
				if(*path != '\\' && !dirflag &&
				   should_fallback_relative_path(adapter_was_ready)){
				return real_ops.TRUNC(path, length);
			}
			return -1;
		}
		return 0;
	}
	else{
		return real_ops.TRUNC(path, length);
	}
}

OP_DEFINE(FTRUNC){
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		return orchfs_ftruncate(file - ORCH_FD_OFFSET, length);
	}
	return real_ops.FTRUNC(file, length);
}

OP_DEFINE(FTRUNC64){
	if(file >= ORCH_FD_OFFSET)
		return orchfs_ftruncate(file - ORCH_FD_OFFSET, (off_t)length);
	return real_ops.FTRUNC64(file, length);
}

/*LINk*/
OP_DEFINE(LINK){
	/* The async protocol does not implement hard links yet.  Never report a
	 * successful link without changing the namespace.  Explicit OrchFS paths
	 * fail deterministically; for an ambiguous relative source, preserve the
	 * wrapper's usual OrchFS-first routing before falling back to libc. */
	if(*path1 == '\\' || *path2 == '\\' ||
	   has_orchfs_prefix(path1) || has_orchfs_prefix(path2)){
		errno = EOPNOTSUPP;
		return -1;
	}
	if(*path1 != '/' || *path2 != '/'){
		struct stat source_stat;
		if(*path1 != '/'){
			int adapter_was_ready = orchfs_async_adapter_ready();
			if(orchfs_stat(path1, &source_stat) == 0){
				errno = EOPNOTSUPP;
				return -1;
			}
			if(!should_fallback_relative_path(adapter_was_ready))
				return -1;
		}
	}
	return real_ops.LINK(path1, path2);
	//printf("path1=%s path2=%s\n",path1,path2);
	// int dirflag=0;
	// if(path1[0]=='/'&&path1[1]=='O'&&path1[2]=='r')
	// 	dirflag=1;
	// if(*path1 == '\\' || *path1 != '/' || dirflag){
	// 	PRINT_FUNC;
	// 	//printf("my link\n");
	// 	char *p=path1;
	// 	if(*path1=='\\') path1=path1+1;
	// 	if(dirflag) path1=path1+3;
	// 	if(path2=='\\') path2=path2+1;
	// 	if(path2[0]=='/'&&path2[1]=='O'&&path2[2]=='r') path2=path2+3;
	// 	if(orchfs_link(path1, path2) == -1){
	// 		if(*p != '\\' && !dirflag){
	// 			return real_ops.LINK(path1, path2);
	// 		}
	// 		return -1;
	// 	}
	// 	return 0;
	// }
	// else{
	// 	return real_ops.LINK(path1, path2);
	// }
}

OP_DEFINE(UNLINK){
	// printf("unlink path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my unlink\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_unlink(path + 1);
		else if(dirflag)
			ret = orchfs_unlink(strip_orchfs_prefix(path));
		else ret = orchfs_unlink(path);
		if(ret == -1){
				if(*path != '\\' && !dirflag &&
				   should_fallback_relative_path(adapter_was_ready)){
				return real_ops.UNLINK(path);
			}
			return -1;
		}
#ifdef orchfs_DEBUG
		printf("unlinked: %s\n", path);
#endif
		return 0;
	}
	else{
		return real_ops.UNLINK(path);
	}
}

OP_DEFINE(FSYNC){
	// printf("fsync call\n");
	// printf("fsync fd=%d\n",file);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		return orchfs_fsync(file - ORCH_FD_OFFSET);
	}
	else{
		return real_ops.FSYNC(file);
	}
}

OP_DEFINE(READ){
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		// printf("read call fd=%d %d\n",file,length);
		int64_t read_len = orchfs_read(file - ORCH_FD_OFFSET, buf, length);
		// char* buf_char = buf;
		// printf("read data: %s\n",buf_char);
		return read_len;
	}
	else{
		if(real_ops.READ == NULL){
			insert_real_op();
		}
		// char* buf_char = buf;
		int64_t read_len = real_ops.READ(file, buf, length);
		// printf("read data: %s\n",buf_char);
		return read_len;
	}
}

OP_DEFINE(READ2){
	// printf("read2 call fd=%d\n",file);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("my read2\n");
		return orchfs_read(file - ORCH_FD_OFFSET, buf, length);
	}
	else{
		return real_ops.READ2(file, buf, length);
	}
}

OP_DEFINE(WRITE){
	// char* buf_char = buf;
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		// PRINT_FUNC;
		// printf("write call fd=%d %d\n",file,length);
		int64_t write_len = orchfs_write(file - ORCH_FD_OFFSET, buf, length);
		// printf("write_len: %d %d\n",file,write_len);
		// char new_buf[4096] = {0};
		// orchfs_pread(file - ORCH_FD_OFFSET, new_buf, write_len, 0);
		// for(int i = 0; i < write_len; i++)
		// {
		// 	printf("info: %d %d %d\n",file,buf_char[i],new_buf[i]);
		// }
		return write_len;
	}
	else{
		return real_ops.WRITE(file, buf, length);
	}
}

OP_DEFINE(PREAD){
	// printf("pread call fd=%d count=%d offset=%d\n",file,count,offset);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		// fprintf(stderr,"my pread\n");
		// fprintf(stderr,"\t\tread: %lu\n", count);
		return orchfs_pread(file - ORCH_FD_OFFSET, buf, count, offset);
	}
	else{
		return real_ops.PREAD(file, buf, count, offset);
	}
}

OP_DEFINE(PREAD64){
	// printf("pread64 call fd=%d count=%d offset=%d\n",file,count,offset);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		// fprintf(stderr,"my pread64\n");
		// fprintf(stderr,"\t\tread: %lu\n", count);
		return orchfs_pread(file - ORCH_FD_OFFSET, buf, count, offset);
	}
	else{
		return real_ops.PREAD64(file, buf, count, offset);
	}
}

OP_DEFINE(PWRITE){
	// char* buf_char = buf;
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("111111\n");
		// printf("pwrite call fd=%d %lld %lld\n",file,count,offset);
		return orchfs_pwrite(file - ORCH_FD_OFFSET, buf, count, offset);
	}
	else{
		//printf("wrapper %d\n",__LINE__);
		// printf("file=%d count=%llu offset=%llu\n",file,count,offset);
		return real_ops.PWRITE(file, buf, count, offset);
	}
}

OP_DEFINE(PWRITE64){
	// fprintf(stderr,"pwrite64 call fd=%d\n",file);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		// fprintf(stderr,"my pwrite64\n");
		//printf("now work in PWRITE64\n");
		return orchfs_pwrite(file - ORCH_FD_OFFSET, buf, count, offset);
	}
	else{
		//printf("wrapper %d\n",__LINE__);
		return real_ops.PWRITE64(file, buf, count, offset);
	}
}

#ifdef _STAT_VER
OP_DEFINE(XSTAT){
	// printf("stat call\n");
	// fflush(stdout);
	// printf("xstat path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my stat\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_stat( path + 1, buf);
		else if(dirflag)
				ret = orchfs_stat(strip_orchfs_prefix(path), buf);
		else ret = orchfs_stat( path , buf);
		if(ret == -1){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.XSTAT(_STAT_VER, path, buf);
			}
			return -1;
		}
		return 0;
	}
	else{
		return real_ops.XSTAT(_STAT_VER, path, buf);
	}
}
#else 
OP_DEFINE(STAT){
	if(real_ops.STAT == NULL){
		insert_real_op();
	}
	// printf("stat call\n");
	// fflush(stdout);
	// printf("stat path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my stat\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_stat( path + 1, buf);
		else if(dirflag)
				ret = orchfs_stat(strip_orchfs_prefix(path), buf);
		else ret = orchfs_stat( path , buf);
		if(ret == -1){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.STAT(path, buf);
			}
			return -1;
		}
		return 0;
	}
	else{
		return real_ops.STAT(path, buf);
	}
}
#endif

OP_DEFINE(STAT64){
	// printf("stat64 call\n");
	// fflush(stdout);
	// fprintf(stderr,"stat64 path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my stat64\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_stat( path + 1, (struct stat*)buf);
		else if(dirflag)
				ret = orchfs_stat(strip_orchfs_prefix(path), (struct stat*)buf);
		else ret = orchfs_stat( path , (struct stat*)buf);
		if(ret == -1){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.STAT64(path, buf);
			}
			return -1;
		}
		return 0;
	}
	else{
		return real_ops.STAT64(path, buf);
	}
}

#ifdef _STAT_VER
OP_DEFINE(FXSTAT){
	// printf("fstat call\n");
	// fflush(stdout);
	// printf("fxfd=%d\n",file);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("my fstat\n");
		return orchfs_fstat(file - ORCH_FD_OFFSET, buf);
	}
	else{
		return real_ops.FXSTAT(_STAT_VER, file, buf);
	}
}
#else 
OP_DEFINE(FSTAT){
	// printf("fstat call\n");
	// fflush(stdout);
	// printf("fd=%d\n",file);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("my fstat\n");
		return orchfs_fstat(file - ORCH_FD_OFFSET, buf);
	}
	else{
		return real_ops.FSTAT(file, buf);
	}
}
#endif

OP_DEFINE(FSTAT64){
	// fflush(stdout);
	// fprintf(stderr,"fd=%d\n",file);
	// fflush(stdout);
	if(file >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("my fstat64\n");
		return orchfs_fstat(file - ORCH_FD_OFFSET, (struct stat*)buf);
	}
	else{
		return real_ops.FSTAT64(file, buf);
	}
}


OP_DEFINE(LSTAT){
	// printf("lstat call\n");
	// fflush(stdout);
	// fprintf(stderr,"lstat path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		PRINT_FUNC;
		//printf("my lstat\n");
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_stat( path + 1,buf);
		else if(dirflag)
				ret = orchfs_stat(strip_orchfs_prefix(path),buf);
		else ret = orchfs_stat( path , buf);
		if(ret == -1){
			if(*path != '\\' && !dirflag &&
			   should_fallback_relative_path(adapter_was_ready)){
				return real_ops.LSTAT(path, buf);
			}
			return -1;
		}
		return 0;
	}
	else{
		return real_ops.LSTAT(path, buf);
	}
}

OP_DEFINE(RENAME){
	const char *original_old = old;
	const char *original_new = new;
	int old_prefix = has_orchfs_prefix(old);
	int new_prefix = has_orchfs_prefix(new);
	int old_is_orch = *old == '\\' || *old != '/' || old_prefix;
	int new_is_orch = *new == '\\' || *new != '/' || new_prefix;
	if(!old_is_orch && !new_is_orch)
		return real_ops.RENAME(old, new);
	if(old_is_orch != new_is_orch){
		errno = EXDEV;
		return -1;
	}
	PRINT_FUNC;
	if(*old=='\\') old++;
	else if(old_prefix) old=strip_orchfs_prefix(old);
	if(*new=='\\') new++;
	else if(new_prefix) new=strip_orchfs_prefix(new);
	int adapter_was_ready = orchfs_async_adapter_ready();
	int result = orchfs_rename(old, new);
	if(result == -1 && *original_old != '\\' && !old_prefix &&
	   *original_new != '\\' && !new_prefix &&
	   should_fallback_mutating_relative_path(adapter_was_ready))
		return real_ops.RENAME(original_old, original_new);
	return result;
}

OP_DEFINE(MKDIR){
	// fprintf(stderr,"mkdir call\n");
	// printf("mkdir path=%s mode=%d\n",path,mode);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' ||dirflag){
		// fprintf(stderr,"my mkdir\n");
		// PRINT_FUNC;
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_mkdir(path + 1, mode);
		else if(dirflag)
				ret = orchfs_mkdir(strip_orchfs_prefix(path), mode);
		else 
			ret = orchfs_mkdir(path , mode);
		// fprintf(stderr,"mkdir over\n");
		if(ret == -1){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.MKDIR(path, mode);
			}
			return -1;
		}
		// fprintf(stderr,"mkdir return\n");
		return 0;
	}
	else{
		// fprintf(stderr,"real mkdir:%s\n",path);
		return real_ops.MKDIR(path, mode);
	}
}

OP_DEFINE(RMDIR){
	// printf("rmdir path=%s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		// PRINT_FUNC;
		int ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_rmdir(path + 1);
		else if(dirflag)
				ret = orchfs_rmdir(strip_orchfs_prefix(path));
		else 
			ret = orchfs_rmdir(path);
		// fprintf(stderr,"mkdir over\n");
		if(ret == -1){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.RMDIR(path);
			}
			return -1;
		}
		// fprintf(stderr,"mkdir return\n");
		return 0;
		//printf("my rmdir\n");
		// if(orchfs_rmdir((*path == '\\')? path + 1 : path) == -1){
		// 	if(*path != '\\'){
		// 		return real_ops.RMDIR(path);
		// 	}
		// 	return -1;
		// }
	}
	else
	{
		return real_ops.RMDIR(path);
	}
}

OP_DEFINE(STATFS){
	int dirflag = has_orchfs_prefix(path);
	if(*path == '\\' || *path != '/' || dirflag){
		const char *adapter_path = *path == '\\' ? path + 1 :
								 dirflag ? strip_orchfs_prefix(path) : path;
		int adapter_was_ready = orchfs_async_adapter_ready();
		int result = orchfs_statfs(adapter_path, buf);
		if(result == -1 && *path != '\\' && !dirflag &&
		   should_fallback_relative_path(adapter_was_ready))
			return real_ops.STATFS(path, buf);
		return result;
	}
	return real_ops.STATFS(path, buf);
}

OP_DEFINE(FSTATFS){
	// printf("fstatfs call\n");
	// fflush(stdout);
	//printf("fd=%d\n",fd);
	if(fd >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		//printf("my fstatfs\n");
		return orchfs_fstatfs(fd - ORCH_FD_OFFSET, buf);
	}
	else{
		return real_ops.FSTATFS(fd, buf);
	}
}

OP_DEFINE(FDATASYNC){
	// printf("fdatasync call\n");
	// fflush(stdout);
	//printf("fd=%d\n",fd);
	if(fd >= ORCH_FD_OFFSET){
		return orchfs_fsync(fd - ORCH_FD_OFFSET);
	}
	else{
		return real_ops.FDATASYNC(fd);
	}
}

OP_DEFINE(FCNTL){
	// printf("fcntl call\n");
	// fflush(stdout);
	// exit(0);
	// printf("fd=%d\n",fd);
	va_list ap;
	if(fd >= ORCH_FD_OFFSET){
		PRINT_FUNC;
		if(cmd == F_SETFD || cmd == F_SETFL){
			va_start(ap, cmd);
			int value = va_arg(ap, int);
			va_end(ap);
			return orchfs_fcntl(fd - ORCH_FD_OFFSET, cmd, value);
		}
		return orchfs_fcntl(fd - ORCH_FD_OFFSET, cmd);
	}
	else{
		int result;
		va_start(ap, cmd);
		if(cmd == F_DUPFD || cmd == F_SETFD || cmd == F_SETFL
#ifdef F_DUPFD_CLOEXEC
		   || cmd == F_DUPFD_CLOEXEC
#endif
#ifdef F_SETOWN
		   || cmd == F_SETOWN
#endif
#ifdef F_SETSIG
		   || cmd == F_SETSIG
#endif
#ifdef F_SETLEASE
		   || cmd == F_SETLEASE
#endif
#ifdef F_NOTIFY
		   || cmd == F_NOTIFY
#endif
#ifdef F_SETPIPE_SZ
		   || cmd == F_SETPIPE_SZ
#endif
#ifdef F_ADD_SEALS
		   || cmd == F_ADD_SEALS
#endif
		){
			int value = va_arg(ap, int);
			result = real_ops.FCNTL(fd, cmd, value);
		}else if(cmd == F_GETLK || cmd == F_SETLK || cmd == F_SETLKW
#ifdef F_OFD_GETLK
			 || cmd == F_OFD_GETLK
#endif
#ifdef F_OFD_SETLK
			 || cmd == F_OFD_SETLK
#endif
#ifdef F_OFD_SETLKW
			 || cmd == F_OFD_SETLKW
#endif
#ifdef F_GETOWN_EX
			 || cmd == F_GETOWN_EX
#endif
#ifdef F_SETOWN_EX
			 || cmd == F_SETOWN_EX
#endif
#ifdef F_GET_RW_HINT
			 || cmd == F_GET_RW_HINT
#endif
#ifdef F_SET_RW_HINT
			 || cmd == F_SET_RW_HINT
#endif
#ifdef F_GET_FILE_RW_HINT
			 || cmd == F_GET_FILE_RW_HINT
#endif
#ifdef F_SET_FILE_RW_HINT
			 || cmd == F_SET_FILE_RW_HINT
#endif
		){
			void *value = va_arg(ap, void *);
			result = real_ops.FCNTL(fd, cmd, value);
		}else{
			result = real_ops.FCNTL(fd, cmd);
		}
		va_end(ap);
		return result;
	}
}

OP_DEFINE(OPENDIR){
	// printf("opendir %s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		// PRINT_FUNC;
		// printf("my opendir\n");
		DIR* ret;
		int adapter_was_ready = orchfs_async_adapter_ready();
		if(*path=='\\')
			ret = orchfs_opendir(path + 1);
		else if(dirflag)
				ret = orchfs_opendir(strip_orchfs_prefix(path));
		else 
			ret = orchfs_opendir(path );
		if(ret == NULL){
			if(*path != '\\' && !dirflag && should_fallback_relative_path(adapter_was_ready)){
				return real_ops.OPENDIR(path);
			}
			return NULL;
		}
		return (DIR*)((uint64_t)ret);
	}
	else{
		if(inited == 0){
			insert_real_op();
		}
		return real_ops.OPENDIR(path);
	}
}

OP_DEFINE(CLOSEDIR){
	// printf("closedir call fd=%d\n",(int)(uint64_t)dirp);
	// fflush(stdout);
	if(orchfs_async_is_directory(dirp)){
		PRINT_FUNC;
		//printf("my closedir\n");
		return orchfs_closedir((DIR*) ((uint64_t)dirp));
	}
	else{
		return real_ops.CLOSEDIR(dirp);
	}
}

OP_DEFINE(READDIR){
	// printf("readdir call, fd=%d\n",(int)(uint64_t)dirp);
	// fflush(stdout);
	if(orchfs_async_is_directory(dirp)){
		PRINT_FUNC;
		// struct dirent* ret = orchfs_readdir((DIR*) ((uint64_t)dirp));
		// printf("my readdir ret %lld %d\n",ret,sizeof(ret));
		// fflush(stdout);
		// return ret;
		return orchfs_readdir((DIR*) ((uint64_t)dirp));
	}
	else{
		return real_ops.READDIR(dirp);
	}
}

OP_DEFINE(READDIR64){
	if(orchfs_async_is_directory(dirp)){
		PRINT_FUNC;
		return orchfs_async_readdir64(dirp);
	}
	return real_ops.READDIR64(dirp);
}

OP_DEFINE(SYNC_FILE_RANGE){
	// printf("sync_file_range call\n");
	// fflush(stdout);
	//printf("fd=%d\n",fd);
	if(fd >= ORCH_FD_OFFSET ){
		PRINT_FUNC;
		(void)offset;
		(void)nbytes;
		(void)flags;
		return orchfs_fsync(fd - ORCH_FD_OFFSET);
	}
	else{
		return real_ops.SYNC_FILE_RANGE(fd, offset, nbytes, flags);
	}
}

// /*******************************************************
//  * File stream functions
//  *******************************************************/
struct orchfs_async_cookie {
	int fd;
};

static ssize_t orchfs_cookie_read(void *cookie, char *buffer, size_t length)
{
	return orchfs_async_read(((struct orchfs_async_cookie*)cookie)->fd,
							 buffer, length);
}

static ssize_t orchfs_cookie_write(void *cookie, const char *buffer,
							  size_t length)
{
	return orchfs_async_write(((struct orchfs_async_cookie*)cookie)->fd,
							  buffer, length);
}

static int orchfs_cookie_seek(void *cookie, off64_t *offset, int whence)
{
	off_t result = orchfs_async_lseek(
		((struct orchfs_async_cookie*)cookie)->fd, (off_t)*offset, whence);
	if(result == (off_t)-1)
		return -1;
	*offset = (off64_t)result;
	return 0;
}

static int orchfs_cookie_close(void *cookie)
{
	struct orchfs_async_cookie *state = cookie;
	int result = orchfs_async_close(state->fd);
	int saved_errno = errno;
	free(state);
	errno = saved_errno;
	return result;
}

static FILE *orchfs_async_fopen(const char *path, const char *mode)
{
	if(mode == NULL || mode[0] == '\0'){
		errno = EINVAL;
		return NULL;
	}
	int flags;
	switch(mode[0]){
	case 'r':
		flags = O_RDONLY;
		break;
	case 'w':
		flags = O_WRONLY | O_CREAT | O_TRUNC;
		break;
	case 'a':
		flags = O_WRONLY | O_CREAT | O_APPEND;
		break;
	default:
		errno = EINVAL;
		return NULL;
	}
	if(strchr(mode, '+') != NULL)
		flags = (flags & ~O_ACCMODE) | O_RDWR;
	if(strchr(mode, 'x') != NULL)
		flags |= O_EXCL;
	if(strchr(mode, 'e') != NULL)
		flags |= O_CLOEXEC;

	struct orchfs_async_cookie *cookie = malloc(sizeof(*cookie));
	if(cookie == NULL)
		return NULL;
	cookie->fd = orchfs_async_open(path, flags, (mode_t)0666);
	if(cookie->fd == -1){
		free(cookie);
		return NULL;
	}
	cookie_io_functions_t functions = {
		.read = orchfs_cookie_read,
		.write = orchfs_cookie_write,
		.seek = orchfs_cookie_seek,
		.close = orchfs_cookie_close,
	};
	FILE *stream = fopencookie(cookie, mode, functions);
	if(stream == NULL){
		int saved_errno = errno;
		(void)orchfs_async_close(cookie->fd);
		free(cookie);
		errno = saved_errno;
	}
	return stream;
}

OP_DEFINE(FOPEN){
	// printf("fopen call! %s\n",path);
	// fflush(stdout);
	int dirflag=0;
	if(has_orchfs_prefix(path))
		dirflag=1;
	if(*path == '\\' || *path != '/' || dirflag){
		FILE * ret = NULL;
		int adapter_was_ready = orchfs_async_adapter_ready();
		//printf("my fopen\n");
		PRINT_FUNC;
		if(*path=='\\')
			ret = orchfs_async_fopen(path + 1, mode);
		else if(dirflag)
				ret = orchfs_async_fopen(strip_orchfs_prefix(path), mode);
		else ret = orchfs_async_fopen(path , mode);
		//ret = _fopen((*path == '\\')? path + 1 : path, mode);
		if(ret == NULL && *path != '\\' && !dirflag &&
		   should_fallback_relative_path(adapter_was_ready)){
			return real_ops.FOPEN(path, mode);
		}
		return ret;
	}
	else{
		return real_ops.FOPEN(path, mode);
	}
}

OP_DEFINE(FOPEN64){
	return fopen(path, mode);
}

/*op*/
OP_DEFINE(FPUTS){
	return real_ops.FPUTS(str, stream);
}
OP_DEFINE(FGETS){
	return real_ops.FGETS(str, n, stream);
}

OP_DEFINE(FWRITE){
	return real_ops.FWRITE(buf, length, nmemb, fp);
}

OP_DEFINE(FREAD){
	return real_ops.FREAD(buf, length, nmemb, fp);
}

OP_DEFINE(FCLOSE){
	return real_ops.FCLOSE(fp);
}

OP_DEFINE(FSEEK){
	return real_ops.FSEEK(fp, offset, whence);
}

OP_DEFINE(FFLUSH){
	return real_ops.FFLUSH(fp);
}

// OP_DEFINE(MMAP){
// 	printf("mmap call d=%d\n",file);
// 	fflush(stdout);
// 	return real_ops.MMAP(addr, len, prot, flags, file, off);
// }

static __attribute__((constructor(200))) void init_method(void)
{
	if(real_ops.OPEN == NULL){
		insert_real_op();
	}
	inited = real_ops.OPEN != NULL;

	// printf("Starting to initialize ctFS. \nInstalling real syscalls...\n");
    // fprintf(stderr,"Real syscall installed. Initializing ORCHFS...\n");
#ifdef ORCHFS_ATOMIC_WRITE
	printf("Atomic write is enabled!\n");
#endif
	if(inited){
		if(orchfs_async_adapter_init() != 0){
			int saved_errno = errno;
			fprintf(stderr,
					"OrchFS async client initialization failed: %s "
					"(endpoint %s)\n",
					strerror(saved_errno),
					getenv("ORCHFS_ASYNC_ENDPOINT") ?
						getenv("ORCHFS_ASYNC_ENDPOINT") :
						"/tmp/orchfs-kfs.sock");
			errno = saved_errno;
		}
		return;
	}
	printf("Fialed to init\n");
	inited = 0;
}

static __attribute__((destructor(200))) void over_method(void)
{
	/* libc may run DSO finalizers before its global stdio cleanup.  Flush while
	 * the cookie callbacks still have a live Runtime/session; otherwise buffered
	 * fwrite data could reach the callback only after adapter shutdown. */
	int saved_errno = errno;
	if(real_ops.FFLUSH != NULL)
		(void)real_ops.FFLUSH(NULL);
	orchfs_async_adapter_shutdown();
	errno = saved_errno;
}

#ifdef __cplusplus
}
#endif
