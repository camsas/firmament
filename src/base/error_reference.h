// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Error reference implementation.

#ifndef FIRMAMENT_BASE_ERROR_REFERENCE_H
#define FIRMAMENT_BASE_ERROR_REFERENCE_H

#include <string>

#include "base/common.h"
#include "base/reference_interface.h"

namespace firmament {

class ErrorReference : public ReferenceInterface {
 public:
  explicit ErrorReference(DataObjectID_t id, const string& reason,
                          const string& details)
    : ReferenceInterface(id), reason_(reason), details_(details) {
    desc_.set_id(id);
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
  virtual inline bool Consumable() { return true; }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Error, id=" << id_ << ", reason='"
                   << reason_ << "', details='" << details_ << "'>";
  }
  // Accessor methods
  inline string const reason() { return reason_; }
  inline string const details() { return details_; }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) const {
    CHECK_EQ(desc.type(), ReferenceDescriptor::ERROR);
    CHECK(desc.has_inline_data());
  }
  void ValidateInternalDescriptor() const {
    CHECK_EQ(id_, desc_.id());
    CHECK_EQ(desc_.type(), ReferenceDescriptor::ERROR);
    CHECK(desc_.has_inline_data());
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

}  // namespace firmament

#endif  // FIRMAMENT_ERROR_REFERENCE_H
