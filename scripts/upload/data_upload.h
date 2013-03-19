// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility to upload data objects into the name store.

#ifndef DATA_UPLOAD_H_
#define DATA_UPLOAD_H_

#include <stdint.h>

#include <camio/camio.h>

#define DIOS_NAME_BITS 256
#define DIOS_NAME_BYTES (DIOS_NAME_BITS/8)
#define DIOS_NAME_QWORDS (DIOS_NAME_BYTES/8)

typedef struct {
  union {
    uint64_t value[DIOS_NAME_QWORDS];
    uint8_t raw[DIOS_NAME_BYTES];
  };
} dios_name_t;

void configure_options();
void term(int signum);

#endif // DATA_UPLOAD_H_
