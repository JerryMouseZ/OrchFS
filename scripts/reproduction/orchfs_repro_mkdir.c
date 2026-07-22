#include <errno.h>
#include <stdio.h>
#include <sys/stat.h>

int main(int argc, char **argv) {
  if (argc != 2) {
    fprintf(stderr, "usage: %s path\n", argv[0]);
    return 2;
  }
  if (mkdir(argv[1], 0700) != 0 && errno != EEXIST) {
    perror("mkdir");
    return 1;
  }
  return 0;
}
