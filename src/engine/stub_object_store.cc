// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub object store class.

#include "engine/stub_object_store.h"

#include "base/common.h"
#include "base/types.h"

namespace firmament {
namespace store {

StubObjectStore::StubObjectStore() {
  LOG(INFO) << "Object store set up: " << *this;
}

bool StubObjectStore::GetObject(DataObjectID_t id, void* data, size_t* len) {
  VLOG(1) << "Retrieving object " << id << " from store.";
  memset(data, 42, 1);
  *len = sizeof(uint32_t);
  return true;
}

void StubObjectStore::PutObject(DataObjectID_t id, void* data, size_t len) {
  VLOG(1) << "Adding object " << id << " (size " << len
          << " bytes) to store.";
}

}  // namespace store
}  // namespace firmament
