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

// Future reference implementation.

#ifndef FIRMAMENT_STORAGE_FUTURE_REFERENCE_H
#define FIRMAMENT_STORAGE_FUTURE_REFERENCE_H

#include "base/common.h"
#include "base/data_object.h"
#include "storage/reference_interface.h"

namespace firmament {

class FutureReference : public ReferenceInterface {
 public:
  explicit FutureReference(DataObjectID_t id)
    : ReferenceInterface(id) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
  }
  explicit FutureReference(const ReferenceDescriptor& desc)
    : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
  }
  virtual inline bool Consumable() const {
    return false;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Future, id=" << id_.name_printable_string() << ">";
  }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) {
    CHECK_EQ(desc.type(), ReferenceDescriptor::FUTURE);
  }
  void ValidateInternalDescriptor() const {
    CHECK_EQ(*id_.name_str(), desc_.id());
    CHECK_EQ(desc_.type(), ReferenceDescriptor::FUTURE);
  }

 private:
  // unit tests
  FRIEND_TEST(ReferencesTest, CreateFutureTest);
  FRIEND_TEST(ReferencesTest, ValidateInternalDescriptors);
  // This field exists for the benefit of easily populating the reference
  // descriptor. It is immutable.
  static const ReferenceType_t type_ = ReferenceDescriptor::FUTURE;
};

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_FUTURE_REFERENCE_H
