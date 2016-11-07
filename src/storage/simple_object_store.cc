/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
