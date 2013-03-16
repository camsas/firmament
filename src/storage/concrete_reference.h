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
          const set<string>& locations)
  : ReferenceInterface(id), size_(size),
  locations_(locations.begin(), locations.end()) {
    desc_.set_id(id.name_bytes(), DIOS_NAME_BYTES);
    desc_.set_type(type_);
    desc_.set_size(size);
    for (set<string>::const_iterator it = locations_.begin();
            it != locations_.end();
            ++it)
      desc_.add_location(*it);
  }
  explicit ConcreteReference(const ReferenceDescriptor& desc)
  : ReferenceInterface(desc) {
    ValidateInitDescriptor(desc);
    size_ = desc.size();
    for (RepeatedPtrField<string>::const_iterator it = desc.location().begin();
            it != desc.location().end();
            ++it)
      locations_.insert(*it);
  }
  void AddLocation(const string& location) {
    locations_.insert(location);
  }
  virtual inline bool Consumable() {
    return true;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Concrete, id=" << id_.name_printable_string()
                   << ", locations={}>";
  }
  inline uint64_t size() {
    return size_;
  }
  inline const set<string>& locations() {
    return locations_;
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
  set<string> locations_;
};

} // namespace firmament

#endif  // FIRMAMENT_STORAGE_CONCRETE_REFERENCE_H
