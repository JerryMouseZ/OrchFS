#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

enum {
    FILE_BYTES = 4 * 1024 * 1024,
    IO_BYTES = 256 * 1024,
};

static unsigned char base_byte(size_t offset)
{
    return (unsigned char)(
        ((uint64_t)offset * UINT64_C(11400714819323198485) >> 37) ^
        ((uint64_t)offset >> 11) ^ UINT64_C(0x6d));
}

static unsigned char expected_byte(size_t offset)
{
    unsigned char value = base_byte(offset);
    if(offset >= 2854777 && offset < 2854777 + 1024)
        value ^= 0xa5;
    if(offset >= 2854777 + 8192 && offset < 2854777 + 8192 + 4096)
        value ^= 0x3c;
    return value;
}

static int write_all_at(int fd, const unsigned char* bytes, size_t length,
                        off_t offset)
{
    while(length != 0)
    {
        ssize_t written = pwrite(fd, bytes, length, offset);
        if(written < 0 && errno == EINTR)
            continue;
        if(written <= 0)
            return -1;
        bytes += written;
        length -= (size_t)written;
        offset += written;
    }
    return 0;
}

static int write_fixture(const char* path, int open_flags,
                         int explicit_fsync)
{
    int fd = open(path, O_CREAT | O_TRUNC | O_RDWR | O_CLOEXEC | open_flags,
                  0644);
    if(fd < 0)
        return -1;
    unsigned char* buffer = malloc(IO_BYTES);
    if(buffer == NULL)
    {
        close(fd);
        errno = ENOMEM;
        return -1;
    }
    for(size_t base = 0; base < FILE_BYTES; base += IO_BYTES)
    {
        for(size_t index = 0; index < IO_BYTES; ++index)
            buffer[index] = base_byte(base + index);
        if(write_all_at(fd, buffer, IO_BYTES, (off_t)base) != 0)
            goto failed;
    }
    for(size_t index = 0; index < 1024; ++index)
        buffer[index] = expected_byte(2854777 + index);
    if(write_all_at(fd, buffer, 1024, 2854777) != 0)
        goto failed;
    for(size_t index = 0; index < 4096; ++index)
        buffer[index] = expected_byte(2854777 + 8192 + index);
    if(write_all_at(fd, buffer, 4096, 2854777 + 8192) != 0)
        goto failed;
    if(explicit_fsync && fsync(fd) != 0)
        goto failed;
    free(buffer);
    return close(fd);

failed:
    {
        int saved = errno;
        free(buffer);
        close(fd);
        errno = saved;
        return -1;
    }
}

static int verify_fixture(const char* path)
{
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if(fd < 0)
        return -1;
    struct stat attributes;
    if(fstat(fd, &attributes) != 0 || attributes.st_size != FILE_BYTES)
    {
        int saved = errno != 0 ? errno : EIO;
        close(fd);
        errno = saved;
        return -1;
    }
    unsigned char* buffer = malloc(IO_BYTES);
    if(buffer == NULL)
    {
        close(fd);
        errno = ENOMEM;
        return -1;
    }
    for(size_t base = 0; base < FILE_BYTES; base += IO_BYTES)
    {
        size_t consumed = 0;
        while(consumed < IO_BYTES)
        {
            ssize_t bytes = pread(fd, buffer + consumed,
                                  IO_BYTES - consumed,
                                  (off_t)(base + consumed));
            if(bytes < 0 && errno == EINTR)
                continue;
            if(bytes <= 0)
            {
                errno = bytes == 0 ? EIO : errno;
                goto failed;
            }
            consumed += (size_t)bytes;
        }
        for(size_t index = 0; index < IO_BYTES; ++index)
        {
            const unsigned char expected = expected_byte(base + index);
            if(buffer[index] != expected)
            {
                fprintf(stderr,
                        "content mismatch at %zu: got 0x%02x expected 0x%02x\n",
                        base + index, buffer[index], expected);
                errno = EIO;
                goto failed;
            }
        }
    }
    free(buffer);
    return close(fd);

failed:
    {
        int saved = errno;
        free(buffer);
        close(fd);
        errno = saved;
        return -1;
    }
}

int main(int argc, char** argv)
{
    if(argc != 3 || (strcmp(argv[1], "write") != 0 &&
                     strcmp(argv[1], "verify") != 0))
    {
        fprintf(stderr, "usage: %s write|verify PATH\n", argv[0]);
        return 2;
    }
    const size_t sync_path_bytes = strlen(argv[2]) + sizeof(".osync");
    char* sync_path = malloc(sync_path_bytes);
    if(sync_path == NULL)
    {
        perror("malloc");
        return 1;
    }
    snprintf(sync_path, sync_path_bytes, "%s.osync", argv[2]);
    int result;
    if(strcmp(argv[1], "write") == 0)
    {
        result = write_fixture(argv[2], 0, 1);
        if(result == 0)
            result = write_fixture(sync_path, O_SYNC, 0);
    }
    else
    {
        result = verify_fixture(argv[2]);
        if(result == 0)
            result = verify_fixture(sync_path);
    }
    free(sync_path);
    if(result != 0)
    {
        perror(argv[1]);
        return 1;
    }
    printf("async_persistence_probe %s: PASS\n", argv[1]);
    return 0;
}
