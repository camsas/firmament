// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub header for object store. This currently supports a very simple in-memory
// key-value store.

#ifndef FIRMAMENT_ENGINE_STUB_OBJECT_STORE_H
#define FIRMAMENT_ENGINE_STUB_OBJECT_STORE_H

#include <string>
#include <map>

#include "base/common.h"
#include "base/types.h"
#include "engine/object_store_interface.h"

namespace firmament {
namespace store {

class StubObjectStore : public ObjectStoreInterface {
 public:
  StubObjectStore();
  void PutObject(DataObjectID_t id, void* data, size_t len);
  bool GetObject(DataObjectID_t id, void*, size_t* len);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<StubObjectStore, containing "
                   << stub_object_map_.size() << " objects>";
  }
 protected:
  map<DataObjectID_t, string> stub_object_map_;
};

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_STUB_OBJECT_STORE_H
