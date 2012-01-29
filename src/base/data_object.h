// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object class definition.

#ifndef FIRMAMENT_DATA_OBJECT_H
#define FIRMAMENT_DATA_OBJECT_H

#include "base/common.h"

namespace firmament {

// XXX: temporary forward-declaration.
struct DataObjectName {};

class DataObject {
 public:
  DataObject(void *buffer, uint64_t length);
  void *buffer() { return buf_ptr_; }
  uint64_t size() { return buf_len_; }
  bool valid() { return (buf_ptr_ != NULL && buf_len_ > 0); }
 private:
  void *buf_ptr_;
  uint64_t buf_len_;
  DataObjectName name_;
};

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_DATA_OBJECT_H
