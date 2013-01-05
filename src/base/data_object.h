// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object class definition. In Firmament, a data object is a chunk of
// binary data, in memory or not, that can be read, written or executed. It is
// globally uniquely named by a DataObjectName.
// Populating buffers for a non-resident data object is a platform-specific
// action and thus implemented in the object store.

#ifndef FIRMAMENT_DATA_OBJECT_H
#define FIRMAMENT_DATA_OBJECT_H

#include "base/common.h"
#include "base/data_object_name.pb.h"

namespace firmament {

class DataObject {
 public:
  DataObject(void *buffer, uint64_t length);
  void *buffer() { return buf_ptr_; }
  void set_buffer(void *buf_ptr, uint64_t len) {
    // N.B.: We are not doing any permission or ACL checking on the buffer
    // pointer here.
    buf_ptr_ = buf_ptr;
    buf_len_ = len;
  }
  uint64_t size() { return buf_len_; }
  bool resident() { return (buf_ptr_ != NULL && buf_len_ > 0); }
 private:
  void *buf_ptr_;
  uint64_t buf_len_;
  DataObjectName name_;
};

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_DATA_OBJECT_H
