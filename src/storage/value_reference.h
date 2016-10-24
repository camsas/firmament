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

// Value reference implementation.

#ifndef FIRMAMENT_STORAGE_VALUE_REFERENCE_H
#define FIRMAMENT_STORAGE_VALUE_REFERENCE_H

#include <string>

#include "base/common.h"
#include "storage/reference_interface.h"

namespace firmament {

class ValueReference : public ReferenceInterface {
 public:
  explicit ValueReference(DataObjectID_t id, const string& value)
  : ReferenceInterface(id), value_(value) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
    desc_.set_inline_data(value);
  }
  explicit ValueReference(const ReferenceDescriptor& desc)
  : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
    value_ = desc.inline_data();
  }
  virtual inline bool Consumable() const {
    return true;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Value, id=" << id_.name_printable_string()
                   << ", value='" << value_ << "'>";
  }
  // Accessor methods
  inline const string value() {
    return value_;
  }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) {
    CHECK_EQ(desc.type(), ReferenceDescriptor::VALUE);
    CHECK(!desc.inline_data().empty());
  }
  void ValidateInternalDescriptor() const {
    CHECK_EQ(*id_.name_str(), desc_.id());
    CHECK_EQ(desc_.type(), ReferenceDescriptor::VALUE);
    CHECK_EQ(desc_.inline_data(), value_);
  }

 private:
  // unit tests
  FRIEND_TEST(ReferencesTest, CreateValueTest);
  FRIEND_TEST(ReferencesTest, ValidateInternalDescriptors);
  // This field exists for the benefit of easily populating the reference
  // descriptor. It is immutable.
  static const ReferenceType_t type_ = ReferenceDescriptor::VALUE;
  // Additional members
  string value_;
};

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_VALUE_REFERENCE_H
