// The Firmament project
// Copyright (c) The Firmament Authors.
// Copyright (c) 2012 Matthew P. Grosvenor  <matthew.grosvenor@cl.cam.ac.uk>
//
// IO helpers for operation of R2D2 examples in standalone mode.

//#ifndef __FIRMAMENT__
#ifndef FIRMAMENT_EXAMPLE_R2D2_STANDALONE_IO_H
#define FIRMAMENT_EXAMPLE_R2D2_STANDALONE_IO_H

#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <memory.h>
#include <sys/stat.h>
#include <errno.h>

#include "examples/r2d2_trace_process/common.h"

#ifdef __cplusplus
extern "C" {
#endif
void* load_to_shmem(char* filename) {
    int fd = open(filename, O_RDONLY);
    if (unlikely(fd < 0)) {
      printf("Could not open file %s: %s\n", filename, strerror(errno));
      _exit(1);
    }

    // Get the file size
    struct stat st;
    stat(filename, &st);
    uint64_t blob_size = st.st_size;

    // Map the whole thing into memory
    void* blob = mmap(NULL, blob_size, PROT_READ, MAP_SHARED, fd, 0);
    if (unlikely(blob == MAP_FAILED)) {
      printf("Could not memory map blob file %s: %s\n", filename,
             strerror(errno));
      _exit(1);
    }
    return blob;
}
#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // FIRMAMENT_EXAMPLE_R2D2_STANDALONE_IO_H
//#endif  // ifndef __FIRMAMENT__
