// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// A set of handy #includes for all of the supported reference types.

#ifndef FIRMAMENT_BASE_REFERENCE_TYPES_H
#define FIRMAMENT_BASE_REFERENCE_TYPES_H

#include "base/common.h"
#include "base/reference_interface.h"

#include "base/future_reference.h"
#include "base/concrete_reference.h"
#include "base/error_reference.h"
#include "base/value_reference.h"

namespace firmament {

shared_ptr<ReferenceInterface> ReferenceFromDescriptor(
    const ReferenceDescriptor& desc) {
  switch (desc.type()) {
    case ReferenceDescriptor::TOMBSTONE:
      //return shared_ptr<ReferenceInterface>(new TombstoneReference(desc));
      LOG(FATAL) << "Unimplemented!";
      break;
    case ReferenceDescriptor::FUTURE:
      return shared_ptr<ReferenceInterface>(new FutureReference(desc));
      break;
    case ReferenceDescriptor::CONCRETE:
      return shared_ptr<ReferenceInterface>(new ConcreteReference(desc));
      break;
    case ReferenceDescriptor::ERROR:
      return shared_ptr<ReferenceInterface>(new ErrorReference(desc));
      break;
    case ReferenceDescriptor::VALUE:
      return shared_ptr<ReferenceInterface>(new ValueReference(desc));
      break;
    default:
      LOG(FATAL) << "Unknown or unrecognized reference type.";
  }
}

}  // namespace firmament

#endif  // FIRMAMENT_BASE_REFERENCE_TYPES_H
