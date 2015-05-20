// The Firmament project
// Copyright (c) 2012-2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2012-2013 Natacha Crooks <natacha.crooks@cl.cam.ac.uk>
//
// Simple in-memory object store class.

#include "storage/simple_object_store.h"

#include <set>

#include "base/data_object.h"
#include "misc/uri_tools.h"

DECLARE_string(listen_uri);

namespace firmament {
namespace store {

// TODO(tach): make non unix specific.

SimpleObjectStore::SimpleObjectStore(ResourceID_t uuid_)
    : ObjectStoreInterface() {
  VLOG(2) << "Constructing simple object store";
  object_table_.reset(new DataObjectMap_t);
}

SimpleObjectStore::~SimpleObjectStore() {
  VLOG(2) << "Destroying simple object store";
  object_table_.reset();
}

} // namespace store
} // namespace firmament
