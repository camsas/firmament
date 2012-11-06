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

}  // namespace store
}  // namespace firmament
