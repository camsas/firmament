// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Future reference implementation.

#ifndef FIRMAMENT_BASE_FUTURE_REFERENCE_H
#define FIRMAMENT_BASE_FUTURE_REFERENCE_H

#include "base/common.h"
#include "base/reference_interface.h"

namespace firmament {

class FutureReference : public ReferenceInterface {
 public:
  explicit FutureReference(DataObjectID_t id)
    : ReferenceInterface(id) {
    desc_.set_id(id);
    desc_.set_type(type_);
  }
  explicit FutureReference(const ReferenceDescriptor& desc)
    : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
  }
  virtual inline bool Consumable() { return false; }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Future, id=" << id_ << ">";
  }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) {
    CHECK_EQ(desc.type(), ReferenceDescriptor::FUTURE);
  }
  void ValidateInternalDescriptor() {
    CHECK_EQ(id_, desc_.id());
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

}  // namespace firmament

#endif  // FIRMAMENT_FUTURE_REFERENCE_H
