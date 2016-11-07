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

// Common reference interface and implementation. This is subclassed by the
// various reference type classes.

#ifndef FIRMAMENT_BASE_REFERENCE_INTERFACE_H
#define FIRMAMENT_BASE_REFERENCE_INTERFACE_H

#include "base/common.h"
#include "base/data_object.h"
#include "base/reference_desc.pb.h"
#include "misc/printable_interface.h"

namespace firmament {

  typedef ReferenceDescriptor::ReferenceType ReferenceType_t;

  typedef DataObject DataObjectID_t;

class ReferenceInterface : public PrintableInterface {
 public:
  // All reference types must defined 2 types of constructor:
  // 1. one taking a DataObjectID_t, and manufacturing a new reference of this
  //    name
  // 2. one taking a ReferenceDescriptor, which acts as a "copy" constructor,
  //    constructing a reference object from the serialized
  //    ReferenceDescriptor.

  explicit ReferenceInterface(DataObjectID_t id)
    : id_(id) {
    desc_.set_id(id_.name_bytes());
  }

  explicit ReferenceInterface(const ReferenceDescriptor& desc)
    : id_(desc.id(), false),
    desc_(desc) {
    }

  // Non-accessor members
  inline ReferenceDescriptor AsProtobuf() {
    return desc();
  }
  virtual bool Consumable() const = 0;
  virtual ostream& ToString(ostream* stream) const = 0;
  // Accessor methods

  inline const DataObjectID_t& id() const {
    return id_;
  }

  inline ReferenceDescriptor desc() const {
    ValidateInternalDescriptor();
    return desc_;
  }

  void set_producing_task(uint64_t tid) {
    desc_.set_producing_task(tid);
  }

 protected:
  virtual void ValidateInitDescriptor(const ReferenceDescriptor& desc) = 0;

  void ValidateInternalDescriptor() const {
    CHECK_EQ(memcmp(id_.name_bytes(), desc_.id().data(), DIOS_NAME_BYTES), 0);
  }
  // Fields shared between all reference types
  DataObjectID_t id_;
  ReferenceDescriptor desc_;
};

} // namespace firmament

#endif  // FIRMAMENT_BASE_REFERENCE_INTERFACE_H
