// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object implementation (high-level).

#include "base/data_object.h"

namespace firmament {

DataObject::DataObject(void *buffer, uint64_t length)
    : buf_ptr_(buffer),
      buf_len_(length) { }

}  // namespace firmament
