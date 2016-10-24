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

// Error reference implementation.

#ifndef FIRMAMENT_STORAGE_ERROR_REFERENCE_H
#define FIRMAMENT_STORAGE_ERROR_REFERENCE_H

#include <string>

#include "base/common.h"
#include "base/data_object.h"
#include "storage/reference_interface.h"

namespace firmament {

class ErrorReference : public ReferenceInterface {
 public:
  explicit ErrorReference(DataObjectID_t id, const string& reason,
                          const string& details)
  : ReferenceInterface(id), reason_(reason), details_(details) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
    desc_.set_inline_data(reason + ": " + details);
  }
  explicit ErrorReference(const ReferenceDescriptor& desc)
  : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
    // XXX(malte): Parse descriptor's inline_data into reason/details!
    LOG(FATAL) << "Intializing Error refs from descriptors is not "
            << "fully implemented yet";
  }
  virtual inline bool Consumable() const {
    return true;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Error, id=" << id_.name_printable_string()
                   << ", reason='" << reason_ << "', details='" << details_
                   << "'>";
  }
  // Accessor methods
  inline string const reason() {
    return reason_;
  }
  inline string const details() {
    return details_;
  }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) {
    CHECK_EQ(desc.type(), ReferenceDescriptor::ERROR);
    CHECK(!desc.inline_data().empty());
  }
  void ValidateInternalDescriptor() const {
    CHECK_EQ(*id_.name_str(), desc_.id());
    CHECK_EQ(desc_.type(), ReferenceDescriptor::ERROR);
    CHECK(!desc_.inline_data().empty());
    CHECK_EQ(desc_.inline_data(), reason_ + ": " + details_);
  }

 private:
  // unit tests
  FRIEND_TEST(ReferencesTest, CreateErrorTest);
  FRIEND_TEST(ReferencesTest, ValidateInternalDescriptors);
  // This field exists for the benefit of easily populating the reference
  // descriptor. It is immutable.
  static const ReferenceType_t type_ = ReferenceDescriptor::ERROR;
  // Additional members
  const string reason_;
  const string details_;
};

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_ERROR_REFERENCE_H
