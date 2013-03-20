// The Firmament project
// Copyright (c) 2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
