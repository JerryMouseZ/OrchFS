#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define ORCH_FD_OFFSET 1048576

static int check_mode(int fd, const char *operation)
{
	struct stat attributes;
	if(fstat(fd, &attributes) != 0)
	{
		perror("fstat");
		return -1;
	}
	if(!S_ISREG(attributes.st_mode) ||
	   (attributes.st_mode & 0777) != 0640)
	{
		fprintf(stderr, "%s mode was %03o, expected 640\n", operation,
			(unsigned)(attributes.st_mode & 0777));
		return -1;
	}
	return 0;
}

static int check_virtual_dirfd_does_not_fallback(void)
{
	const int virtual_dirfd = ORCH_FD_OFFSET + 123;
	errno = 0;
	int fd = openat(virtual_dirfd, "missing", O_RDONLY | O_CLOEXEC);
	if(fd >= 0)
	{
		fprintf(stderr, "openat unexpectedly accepted a fake OrchFS dirfd\n");
		(void)close(fd);
		return -1;
	}
	if(errno != ENOENT)
	{
		fprintf(stderr,
			"openat on an OrchFS dirfd returned %d, expected ENOENT\n",
			errno);
		return -1;
	}

	errno = 0;
	fd = openat(virtual_dirfd, "missing", O_CREAT | O_EXCL | O_RDWR,
		0640);
	if(fd >= 0)
	{
		fprintf(stderr,
			"openat(O_CREAT) unexpectedly accepted a fake OrchFS dirfd\n");
		(void)close(fd);
		return -1;
	}
	if(errno != ENOENT)
	{
		fprintf(stderr,
			"openat(O_CREAT) on an OrchFS dirfd returned %d, expected ENOENT\n",
			errno);
		return -1;
	}
	return 0;
}

static int check_at_fdcwd_fallback(const char *directory, int directory_fd)
{
	static const char filename[] = "at-fdcwd-fallback";
	int cwd_fd = open("/proc/self/cwd",
		O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if(cwd_fd < 0)
	{
		perror("open(/proc/self/cwd)");
		return -1;
	}
	if(chdir(directory) != 0)
	{
		perror("chdir");
		(void)close(cwd_fd);
		return -1;
	}

	mode_t old_mask = umask(0);
	int fd = openat(AT_FDCWD, filename,
		O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0640);
	int open_error = errno;
	int restore_result = fchdir(cwd_fd);
	int restore_error = errno;
	umask(old_mask);
	(void)close(cwd_fd);
	if(restore_result != 0)
	{
		errno = restore_error;
		perror("fchdir");
		if(fd >= 0)
			(void)close(fd);
		return -1;
	}
	if(fd < 0)
	{
		errno = open_error;
		perror("openat(AT_FDCWD fallback)");
		return -1;
	}

	int result = check_mode(fd, "openat(AT_FDCWD fallback)");
	if(close(fd) != 0)
	{
		perror("close(AT_FDCWD fallback)");
		result = -1;
	}
	if(unlinkat(directory_fd, filename, 0) != 0)
	{
		perror("unlinkat(AT_FDCWD fallback)");
		result = -1;
	}
	return result;
}

int main(void)
{
	if(check_virtual_dirfd_does_not_fallback() != 0)
		return 1;

#ifndef O_TMPFILE
	return 77;
#else
	char directory[] = "/tmp/orchfs-wrapper-host-XXXXXX";
	if(mkdtemp(directory) == NULL)
	{
		perror("mkdtemp");
		return 1;
	}

	int directory_fd = open(directory, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
	if(directory_fd < 0)
	{
		perror("open(directory)");
		(void)rmdir(directory);
		return 1;
	}
	if(check_at_fdcwd_fallback(directory, directory_fd) != 0)
	{
		(void)close(directory_fd);
		(void)rmdir(directory);
		return 1;
	}

	mode_t old_mask = umask(0);
	int fd = open(directory, O_TMPFILE | O_RDWR | O_CLOEXEC, 0640);
	int open_error = errno;
	if(fd < 0)
	{
		umask(old_mask);
		(void)close(directory_fd);
		(void)rmdir(directory);
		if(open_error == EOPNOTSUPP || open_error == ENOTSUP ||
		   open_error == EISDIR)
			return 77;
		errno = open_error;
		perror("open(O_TMPFILE)");
		return 1;
	}

	int relative_fd = openat(directory_fd, ".",
		O_TMPFILE | O_RDWR | O_CLOEXEC, 0640);
	int openat_error = errno;
	umask(old_mask);
	if(relative_fd < 0)
	{
		errno = openat_error;
		perror("openat(O_TMPFILE)");
		(void)close(fd);
		(void)close(directory_fd);
		(void)rmdir(directory);
		return 1;
	}

	if(check_mode(fd, "open(O_TMPFILE)") != 0 ||
	   check_mode(relative_fd, "openat(O_TMPFILE)") != 0)
	{
		(void)close(relative_fd);
		(void)close(fd);
		(void)close(directory_fd);
		(void)rmdir(directory);
		return 1;
	}

	if(close(relative_fd) != 0 || close(fd) != 0 ||
	   close(directory_fd) != 0 || rmdir(directory) != 0)
	{
		perror("cleanup");
		return 1;
	}
	return 0;
#endif
}
