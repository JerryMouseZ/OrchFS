#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

enum {
  COPY_ALIGNMENT = 4096,
  COPY_CHUNK = 1024 * 1024,
  ORCHFS_FD_OFFSET = 1048576,
};

static int write_all(int fd, const void *buffer, size_t length, off_t offset) {
  const unsigned char *cursor = buffer;
  size_t written = 0;
  while (written < length) {
    ssize_t result = pwrite(fd, cursor + written, length - written,
                            offset + (off_t)written);
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      return -1;
    }
    written += (size_t)result;
  }
  return 0;
}

static int read_all(int fd, void *buffer, size_t length, off_t offset) {
  unsigned char *cursor = buffer;
  size_t received = 0;
  while (received < length) {
    ssize_t result = pread(fd, cursor + received, length - received,
                           offset + (off_t)received);
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      return -1;
    }
    received += (size_t)result;
  }
  return 0;
}

static int copy_one(const char *source_path, const char *destination_path,
                    unsigned char *buffer, unsigned char *verify_buffer) {
  struct stat source_stat;
  off_t offset = 0;
  int source = -1;
  int destination = -1;
  int status = 1;

  source = open(source_path, O_RDONLY);
  if (source < 0 || fstat(source, &source_stat) != 0 ||
      !S_ISREG(source_stat.st_mode)) {
    perror("open/fstat source");
    goto out;
  }
  destination = open(destination_path, O_RDWR | O_CREAT | O_EXCL, 0600);
  if (destination < 0) {
    perror("open destination");
    goto out;
  }
  if (destination < ORCHFS_FD_OFFSET) {
    int saved_errno = EXDEV;
    close(destination);
    destination = -1;
    (void)unlink(destination_path);
    errno = saved_errno;
    perror("destination did not resolve to OrchFS");
    goto out;
  }
  while (offset < source_stat.st_size) {
    off_t remaining = source_stat.st_size - offset;
    size_t wanted = remaining < COPY_CHUNK ? (size_t)remaining : COPY_CHUNK;
    size_t received = 0;
    while (received < wanted) {
      ssize_t result = pread(source, buffer + received, wanted - received,
                             offset + (off_t)received);
      if (result < 0 && errno == EINTR) {
        continue;
      }
      if (result <= 0) {
        perror("pread source");
        goto out;
      }
      received += (size_t)result;
    }
    size_t submitted = wanted;
    if (wanted % COPY_ALIGNMENT != 0) {
      submitted = (wanted + COPY_ALIGNMENT - 1) & ~(COPY_ALIGNMENT - 1);
      memset(buffer + wanted, 0, submitted - wanted);
    }
    if (write_all(destination, buffer, submitted, offset) != 0) {
      perror("pwrite destination");
      goto out;
    }
    offset += (off_t)wanted;
  }
  if (ftruncate(destination, source_stat.st_size) != 0) {
    perror("ftruncate destination");
    goto out;
  }
  if (fsync(destination) != 0) {
    perror("fsync destination");
    goto out;
  }
  offset = 0;
  while (offset < source_stat.st_size) {
    off_t remaining = source_stat.st_size - offset;
    size_t wanted = remaining < COPY_CHUNK ? (size_t)remaining : COPY_CHUNK;
    if (read_all(source, buffer, wanted, offset) != 0) {
      perror("verify pread source");
      goto out;
    }
    if (read_all(destination, verify_buffer, wanted, offset) != 0) {
      perror("verify pread destination");
      goto out;
    }
    if (memcmp(buffer, verify_buffer, wanted) != 0) {
      errno = EIO;
      perror("verify destination mismatch");
      goto out;
    }
    offset += (off_t)wanted;
  }
  fprintf(stderr, "orchfs_repro_copy bytes=%lld verified=yes source=%s destination=%s\n",
          (long long)source_stat.st_size, source_path, destination_path);
  status = 0;

out:
  if (destination >= 0 && close(destination) != 0 && status == 0) {
    perror("close destination");
    status = 1;
  }
  if (source >= 0) {
    close(source);
  }
  return status;
}

int main(int argc, char **argv) {
  unsigned char *buffer = NULL;
  unsigned char *verify_buffer = NULL;
  int first_pair = 1;
  int status = 0;

  if (argc >= 3 && strcmp(argv[1], "--mkdir") == 0) {
    if (mkdir(argv[2], 0700) != 0 && errno != EEXIST) {
      perror("mkdir destination root");
      return 1;
    }
    first_pair = 3;
  }
  if (argc - first_pair < 2 || (argc - first_pair) % 2 != 0) {
    fprintf(stderr,
            "usage: %s [--mkdir orchfs-directory] "
            "host-source orchfs-destination "
            "[host-source orchfs-destination ...]\n",
            argv[0]);
    return 2;
  }
  if (posix_memalign((void **)&buffer, COPY_ALIGNMENT, COPY_CHUNK) != 0) {
    fprintf(stderr, "posix_memalign failed\n");
    return 1;
  }
  if (posix_memalign((void **)&verify_buffer, COPY_ALIGNMENT, COPY_CHUNK) != 0) {
    fprintf(stderr, "posix_memalign verify buffer failed\n");
    free(buffer);
    return 1;
  }
  for (int index = first_pair; index < argc; index += 2) {
    if (copy_one(argv[index], argv[index + 1], buffer, verify_buffer) != 0) {
      status = 1;
      break;
    }
  }
  free(verify_buffer);
  free(buffer);
  return status;
}
