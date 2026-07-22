#define _GNU_SOURCE

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { EDGE_BATCH = 8192 };

static int parse_vertex(char **cursor, uint32_t *value) {
  char *end = NULL;
  unsigned long parsed;

  errno = 0;
  parsed = strtoul(*cursor, &end, 10);
  if (errno != 0 || end == *cursor || parsed > UINT32_MAX) {
    return -1;
  }
  *cursor = end;
  *value = (uint32_t)parsed;
  return 0;
}

int main(void) {
  uint32_t edges[EDGE_BATCH][2];
  size_t buffered = 0;
  uint64_t count = 0;
  char *line = NULL;
  size_t capacity = 0;
  ssize_t length;

  while ((length = getline(&line, &capacity, stdin)) >= 0) {
    char *cursor = line;
    uint32_t source;
    uint32_t target;

    (void)length;
    while (*cursor == ' ' || *cursor == '\t') {
      ++cursor;
    }
    if (*cursor == '#' || *cursor == '\n' || *cursor == '\0') {
      continue;
    }
    if (parse_vertex(&cursor, &source) != 0) {
      fprintf(stderr, "invalid source vertex at input line %" PRIu64 "\n",
              count + 1);
      free(line);
      return 1;
    }
    while (*cursor == ' ' || *cursor == '\t') {
      ++cursor;
    }
    if (parse_vertex(&cursor, &target) != 0) {
      fprintf(stderr, "invalid target vertex at input line %" PRIu64 "\n",
              count + 1);
      free(line);
      return 1;
    }
    edges[buffered][0] = source;
    edges[buffered][1] = target;
    ++buffered;
    ++count;
    if (buffered == EDGE_BATCH) {
      if (fwrite(edges, sizeof(edges[0]), buffered, stdout) != buffered) {
        perror("fwrite");
        free(line);
        return 1;
      }
      buffered = 0;
    }
  }
  if (ferror(stdin)) {
    perror("getline");
    free(line);
    return 1;
  }
  if (buffered != 0 &&
      fwrite(edges, sizeof(edges[0]), buffered, stdout) != buffered) {
    perror("fwrite");
    free(line);
    return 1;
  }
  if (fflush(stdout) != 0) {
    perror("fflush");
    free(line);
    return 1;
  }
  fprintf(stderr, "gridgraph_edge_converter edges=%" PRIu64 " bytes=%" PRIu64
                  "\n",
          count, count * (uint64_t)sizeof(edges[0]));
  free(line);
  return 0;
}
