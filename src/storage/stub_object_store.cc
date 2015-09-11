// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub object store class.

#include "storage/stub_object_store.h"

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"

namespace firmament {
namespace store {

StubObjectStore::StubObjectStore() {
  LOG(INFO) << "Object store set up: " << *this;
}

uint64_t StubObjectStore::Flush() {
  // TODO(malte): Lock the store table before doing this?
  uint64_t num_objs = stub_object_map_.size();
  for (map<DataObjectID_t, char*>::const_iterator obj_iter =
       stub_object_map_.begin();
       obj_iter != stub_object_map_.end();
       ++obj_iter) {
    // Dealloc memory
    delete obj_iter->second;
  }
  stub_object_map_.clear();
  CHECK_EQ(stub_object_map_.size(), 0);
  VLOG(1) << "Flushed object stored: removed " << num_objs << " objects!";
  return num_objs;
}

void* StubObjectStore::GetObject(DataObjectID_t id) {
  VLOG(1) << "Retrieving object " << id << " from store.";
  char** data_ptr = FindOrNull(stub_object_map_, id);
  if (data_ptr) {
    return reinterpret_cast<void*>(*data_ptr);
  } else {
    return NULL;
  }
}

void StubObjectStore::PutObject(DataObjectID_t id, void* data, size_t len) {
  VLOG(1) << "Adding object " << id << " (size " << len
          << " bytes) to store.";
  char* store_data = new char[len];
  memcpy(store_data, data, len);
  InsertIfNotPresent(&stub_object_map_, id, store_data);
}

}  // namespace store
}  // namespace firmament
