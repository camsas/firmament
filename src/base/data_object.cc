// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Data object implementation (high-level).

#include "base/data_object.h"

#include <string>

namespace firmament {

DataObject::DataObject(const dios_name_t& name)
  : name_(name) {
}

DataObject::DataObject(const string& name) {
  // Check length
  CHECK(name.size() == DIOS_NAME_BYTES) << "Name length wrong: " << name.size()
                                        << " bytes (expected: "
                                        << DIOS_NAME_BYTES << " bytes)";
  // Set the name
  memcpy(name_.raw, name.data(), DIOS_NAME_BYTES);
}


bool DataObject::operator==(const DataObject& other_do) const {
  return (memcmp(name_.raw, other_do.name_.raw, DIOS_NAME_BYTES) == 0);
}

bool DataObject::operator<(const DataObject& other_do) const {
  return (memcmp(name_.raw, other_do.name_.raw, DIOS_NAME_BYTES) < 0);
}

DataObject::DataObject(const char* name) {
  DataObject(reinterpret_cast<const uint8_t*>(name));
}

DataObject::DataObject(const uint8_t* name) {
  // Set the name
  for (uint32_t i = 0; i < DIOS_NAME_BYTES; ++i)
    name_.raw[i] = name[i];
}


}  // namespace firmament
