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

// A set of helper functions to deal with references.

#ifndef FIRMAMENT_STORAGE_REFERENCE_UTILS_H
#define FIRMAMENT_STORAGE_REFERENCE_UTILS_H

#include "base/common.h"
#include "storage/reference_interface.h"

#include "storage/reference_types.h"

namespace firmament {

static ReferenceInterface* ReferenceFromDescriptor(
    const ReferenceDescriptor& desc) {
  switch (desc.type()) {
    case ReferenceDescriptor::TOMBSTONE:
      //return shared_ptr<ReferenceInterface>(new TombstoneReference(desc));
      LOG(FATAL) << "Unimplemented!";
      break;
    case ReferenceDescriptor::FUTURE:
      return new FutureReference(desc);
      break;
    case ReferenceDescriptor::CONCRETE:
      return new ConcreteReference(desc);
      break;
    case ReferenceDescriptor::ERROR:
      return new ErrorReference(desc);
      break;
    case ReferenceDescriptor::VALUE:
      return new ValueReference(desc);
      break;
    default:
      LOG(FATAL) << "Unknown or unrecognized reference type.";
      // g++ requires this, as it otherwise warns control reaching the end of a
      // non-void function. clang recognizes that LOG(FATAL) exits.
      return NULL; // N.B.: unreachable
  }
}

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_REFERENCE_UTILS_H
