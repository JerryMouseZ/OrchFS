#define _GNU_SOURCE

#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

static void fail(const char* operation)
{
    perror(operation);
    exit(1);
}

static void make_directory(const char* path)
{
    if(mkdir(path, 0755) != 0)
        fail(path);
}

static void make_file(const char* path, const char* contents)
{
    int fd = open(path, O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, 0644);
    if(fd < 0)
        fail(path);
    size_t length = strlen(contents);
    if(write(fd, contents, length) != (ssize_t)length || fsync(fd) != 0 ||
       close(fd) != 0)
        fail("write fixture");
}

static void expect_contents(int fd, const char* expected)
{
    char buffer[32] = {0};
    if(lseek(fd, 0, SEEK_SET) != 0)
        fail("lseek");
    ssize_t bytes = read(fd, buffer, sizeof(buffer));
    if(bytes != (ssize_t)strlen(expected) ||
       memcmp(buffer, expected, (size_t)bytes) != 0)
    {
        fprintf(stderr, "content mismatch: got '%.*s', expected '%s'\n",
                (int)(bytes > 0 ? bytes : 0), buffer, expected);
        exit(1);
    }
}

static void expect_rename_error(const char* old_path, const char* new_path,
                                int expected)
{
    errno = 0;
    if(rename(old_path, new_path) != -1 || errno != expected)
    {
        fprintf(stderr, "rename(%s, %s) returned errno %d, expected %d\n",
                old_path, new_path, errno, expected);
        exit(1);
    }
}

static void test_basic_file_api(void)
{
    const char* path = "/Or/semantics/basic";
    int fd = creat(path, 0640);
    if(fd < 0)
        fail("creat");
    if(close(fd) != 0)
        fail("close creat");

    fd = open(path, O_RDWR | O_TRUNC | O_CLOEXEC);
    if(fd < 0)
        fail("open basic");
    if(fcntl(fd, F_GETFD) != FD_CLOEXEC)
        fail("F_GETFD");
    if(fcntl(fd, F_SETFD, 0) != 0 || fcntl(fd, F_GETFD) != 0)
        fail("F_SETFD");
    int status_flags = fcntl(fd, F_GETFL);
    if(status_flags < 0 || (status_flags & O_ACCMODE) != O_RDWR)
        fail("F_GETFL");

    static const char initial[] = "abcdef";
    if(write(fd, initial, sizeof(initial) - 1) !=
       (ssize_t)(sizeof(initial) - 1))
        fail("write basic");
    static const char replacement[] = "XY";
    if(pwrite(fd, replacement, sizeof(replacement) - 1, 2) !=
       (ssize_t)(sizeof(replacement) - 1))
        fail("pwrite basic");
    char positioned[3] = {0};
    if(pread(fd, positioned, 2, 2) != 2 || strcmp(positioned, "XY") != 0)
        fail("pread basic");
    if(lseek(fd, 0, SEEK_SET) != 0)
        fail("lseek basic");
    char sequential[7] = {0};
    if(read(fd, sequential, 6) != 6 || strcmp(sequential, "abXYef") != 0)
        fail("read basic");

    struct stat descriptor_stat;
    struct stat path_stat;
    struct stat link_stat;
    if(fstat(fd, &descriptor_stat) != 0 ||
       stat(path, &path_stat) != 0 || lstat(path, &link_stat) != 0 ||
       descriptor_stat.st_size != 6 ||
       descriptor_stat.st_ino != path_stat.st_ino ||
       path_stat.st_ino != link_stat.st_ino || !S_ISREG(path_stat.st_mode))
        fail("stat family");
    if(access(path, F_OK | R_OK | W_OK) != 0)
        fail("access");

    struct statfs descriptor_fs;
    struct statfs path_fs;
    if(fstatfs(fd, &descriptor_fs) != 0 || statfs(path, &path_fs) != 0 ||
       descriptor_fs.f_bsize <= 0 ||
       descriptor_fs.f_bsize != path_fs.f_bsize)
        fail("statfs family");

    if(fcntl(fd, F_SETFL, O_APPEND | O_NONBLOCK) != 0)
        fail("F_SETFL");
    status_flags = fcntl(fd, F_GETFL);
    if(status_flags < 0 || (status_flags & O_ACCMODE) != O_RDWR ||
       (status_flags & (O_APPEND | O_NONBLOCK)) !=
           (O_APPEND | O_NONBLOCK))
        fail("F_SETFL result");
    if(write(fd, "!", 1) != 1)
        fail("append write");
    if(fdatasync(fd) != 0 || sync_file_range(fd, 0, 0, 0) != 0)
        fail("data synchronization");
    if(ftruncate(fd, 4) != 0 || fstat(fd, &descriptor_stat) != 0 ||
       descriptor_stat.st_size != 4)
        fail("ftruncate");
    if(close(fd) != 0)
        fail("close basic");
    if(truncate(path, 2) != 0 || stat(path, &path_stat) != 0 ||
       path_stat.st_size != 2)
        fail("truncate");
    if(unlink(path) != 0)
        fail("unlink basic");
    errno = 0;
    if(stat(path, &path_stat) != -1 || errno != ENOENT)
        fail("unlink visibility");
}

static void test_openat_api(void)
{
    int directory = open("/Or/semantics/a",
                         O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if(directory < 0)
        fail("open directory fd");
    int fd = openat(directory, "relative", O_CREAT | O_EXCL | O_RDWR, 0644);
    if(fd < 0 || write(fd, "openat", 6) != 6 || close(fd) != 0)
        fail("openat relative");
    fd = openat64(directory, "relative", O_RDONLY | O_CLOEXEC);
    char contents[7] = {0};
    if(fd < 0 || read(fd, contents, 6) != 6 ||
       strcmp(contents, "openat") != 0 || close(fd) != 0)
        fail("openat64 relative");
    if(close(directory) != 0 || unlink("/Or/semantics/a/relative") != 0)
        fail("openat cleanup");
}

static void test_directory_api(void)
{
    const char* directory_path = "/Or/semantics/listing";
    make_directory(directory_path);
    make_file("/Or/semantics/listing/one", "1");
    make_file("/Or/semantics/listing/two", "2");

    DIR* directory = opendir(directory_path);
    if(directory == NULL)
        fail("opendir");
    int saw_one = 0;
    int saw_two = 0;
    struct dirent* entry;
    errno = 0;
    while((entry = readdir(directory)) != NULL)
    {
        saw_one |= strcmp(entry->d_name, "one") == 0;
        saw_two |= strcmp(entry->d_name, "two") == 0;
    }
    if(errno != 0 || !saw_one || !saw_two || closedir(directory) != 0)
        fail("readdir");

    directory = opendir(directory_path);
    if(directory == NULL)
        fail("opendir readdir64");
    saw_one = 0;
    saw_two = 0;
    struct dirent64* entry64;
    errno = 0;
    while((entry64 = readdir64(directory)) != NULL)
    {
        saw_one |= strcmp(entry64->d_name, "one") == 0;
        saw_two |= strcmp(entry64->d_name, "two") == 0;
    }
    if(errno != 0 || !saw_one || !saw_two || closedir(directory) != 0)
        fail("readdir64");

    if(unlink("/Or/semantics/listing/one") != 0 ||
       unlink("/Or/semantics/listing/two") != 0 ||
       rmdir(directory_path) != 0)
        fail("directory cleanup");
}

static void test_stdio_api(void)
{
    const char* path = "/Or/semantics/stdio";
    FILE* stream = fopen(path, "w+");
    if(stream == NULL)
        fail("fopen w+");
    if(fputs("abc", stream) < 0 || fwrite("def", 1, 3, stream) != 3 ||
       fflush(stream) != 0 || fseek(stream, 0, SEEK_SET) != 0)
        fail("stdio write/seek");
    char contents[7] = {0};
    if(fread(contents, 1, 6, stream) != 6 ||
       strcmp(contents, "abcdef") != 0 || fclose(stream) != 0)
        fail("stdio read/close");

    stream = fopen(path, "r");
    char line[8] = {0};
    if(stream == NULL || fgets(line, sizeof(line), stream) == NULL ||
       strcmp(line, "abcdef") != 0 || fclose(stream) != 0 ||
       unlink(path) != 0)
        fail("stdio reopen");
}

static void test_synchronous_open_flags(void)
{
    int fd = open("/Or/semantics/osync",
                  O_CREAT | O_EXCL | O_RDWR | O_SYNC, 0644);
    if(fd < 0 || write(fd, "sync", 4) != 4 ||
       (fcntl(fd, F_GETFL) & O_SYNC) != O_SYNC || close(fd) != 0)
        fail("O_SYNC");
#ifdef O_DSYNC
    fd = open("/Or/semantics/odsync",
              O_CREAT | O_EXCL | O_RDWR | O_DSYNC, 0644);
    if(fd < 0 || pwrite(fd, "dsync", 5, 0) != 5 ||
       (fcntl(fd, F_GETFL) & O_DSYNC) != O_DSYNC || close(fd) != 0)
        fail("O_DSYNC");
#endif
}

int main(void)
{
    make_directory("/Or/semantics");
    make_directory("/Or/semantics/a");
    make_directory("/Or/semantics/b");

    test_basic_file_api();
    test_openat_api();
    test_directory_api();
    test_stdio_api();
    test_synchronous_open_flags();

    make_file("/Or/semantics/a/source", "new");
    make_file("/Or/semantics/b/target", "old");
    int old_target = open("/Or/semantics/b/target", O_RDONLY | O_CLOEXEC);
    if(old_target < 0)
        fail("open old target");
    if(rename("/Or/semantics/a/source", "/Or/semantics/b/target") != 0)
        fail("atomic replacement rename");
    expect_contents(old_target, "old");
    int replacement = open("/Or/semantics/b/target", O_RDONLY | O_CLOEXEC);
    if(replacement < 0)
        fail("open replacement");
    expect_contents(replacement, "new");
    if(close(replacement) != 0 || close(old_target) != 0)
        fail("close replacement fixtures");
    if(rename("/Or/semantics/b/target", "/Or/semantics/b/target") != 0)
        fail("same-path rename");

    make_directory("/Or/semantics/a/tree");
    make_file("/Or/semantics/a/tree/child", "child");
    if(rename("/Or/semantics/a/tree", "/Or/semantics/b/tree") != 0)
        fail("cross-directory directory rename");
    struct stat attributes;
    errno = 0;
    if(stat("/Or/semantics/a/tree", &attributes) != -1 || errno != ENOENT)
        fail("old directory name remained visible");
    if(stat("/Or/semantics/b/tree/child", &attributes) != 0)
        fail("moved child lookup");
    make_file("/Or/semantics/b/tree/../parent-marker", "parent");
    if(stat("/Or/semantics/b/parent-marker", &attributes) != 0)
        fail("moved directory dotdot update");
    expect_rename_error("/Or/semantics/b",
                        "/Or/semantics/b/tree/nested", EINVAL);

    make_directory("/Or/semantics/a/empty-source");
    make_directory("/Or/semantics/b/nonempty-target");
    make_file("/Or/semantics/b/nonempty-target/child", "x");
    expect_rename_error("/Or/semantics/a/empty-source",
                        "/Or/semantics/b/nonempty-target", ENOTEMPTY);
    make_file("/Or/semantics/a/regular", "regular");
    make_directory("/Or/semantics/b/directory");
    expect_rename_error("/Or/semantics/a/regular",
                        "/Or/semantics/b/directory", EISDIR);
    make_directory("/Or/semantics/a/directory");
    make_file("/Or/semantics/b/regular", "regular");
    expect_rename_error("/Or/semantics/a/directory",
                        "/Or/semantics/b/regular", ENOTDIR);

    printf("async_posix_probe: PASS\n");
    return 0;
}
