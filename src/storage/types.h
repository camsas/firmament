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

// Type definitions used by the object store.

#ifndef FIRMAMENT_STORAGE_TYPES_H
#define FIRMAMENT_STORAGE_TYPES_H

#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <thread_safe_map.h>

#include "storage/reference_interface.h"

namespace firmament {
namespace store {

typedef thread_safe::map<DataObjectID_t, unordered_set<ReferenceInterface*> >
    DataObjectMap_t;

}  // namespace store
}  // namespace firmament

#endif  // FIRMAMENT_STORAGE_TYPES_H
