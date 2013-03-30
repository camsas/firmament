// The Firmament project
// Copyright (c) 2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Concrete reference implementation.

#ifndef FIRMAMENT_STORAGE_CONCRETE_REFERENCE_H
#define FIRMAMENT_STORAGE_CONCRETE_REFERENCE_H

#include <string>
#include <set>
#include <google/protobuf/repeated_field.h>

#include "base/common.h"
#include "storage/reference_interface.h"
#include "storage/future_reference.h"

namespace firmament {

using google::protobuf::RepeatedPtrField;

class ConcreteReference : public ReferenceInterface {
 public:
  explicit ConcreteReference(DataObjectID_t id)
    : ReferenceInterface(id), size_(0) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
  }
  ConcreteReference(DataObjectID_t id, uint64_t size,
                    const string& location)
    : ReferenceInterface(id), size_(size),
      location_(location) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
    desc_.set_size(size);
    desc_.set_location(location);
  }
  explicit ConcreteReference(const FutureReference& fut)
    : ReferenceInterface(fut.id()) {
      desc_.CopyFrom(fut.desc());
      desc_.set_type(ReferenceDescriptor::CONCRETE);
      if (desc_.has_location())
        location_ = desc_.location();
      if (desc_.has_size())
        size_ = desc_.size();
  }
  explicit ConcreteReference(const ReferenceDescriptor& desc)
  : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
    size_ = desc.size();
    location_ = desc.location();
  }
  void SetLocation(const string& location) {
    location_ = location;
  }
  virtual inline bool Consumable() const {
    return true;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Concrete, id=" << id_.name_printable_string()
                   << ", locations={}>";
  }
  inline uint64_t size() {
    return size_;
  }
  inline const string& location() {
    return location_;
  }

 protected:
  void ValidateInitDescriptor(const ReferenceDescriptor& desc) {
    CHECK_EQ(desc.type(), ReferenceDescriptor::CONCRETE);
  }

  void ValidateInternalDescriptor() const {
    CHECK_EQ(*id_.name_str(), desc_.id());
    CHECK_EQ(desc_.type(), ReferenceDescriptor::CONCRETE);
    CHECK_EQ(desc_.size(), size_);
  }

 private:
  // unit tests
  FRIEND_TEST(ReferencesTest, CreateConcreteTest);
  FRIEND_TEST(ReferencesTest, ValidateInternalDescriptors);
  // This field exists for the benefit of easily populating the reference
  // descriptor. It is immutable.
  static const ReferenceType_t type_ = ReferenceDescriptor::CONCRETE;
  // Additional members of concrete refs
  uint64_t size_;
  string location_;
};

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_CONCRETE_REFERENCE_H
