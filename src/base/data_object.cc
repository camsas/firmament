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

// Data object implementation (high-level).

#include "base/data_object.h"

#include <string>

namespace firmament {

DataObject::DataObject(const dios_name_t& name)
  : name_(name) {
}

DataObject::DataObject(const string& name, bool hex_decode) {
  if (hex_decode) {
    uint8_t name_dec[DIOS_NAME_BYTES];
    // Check length
    CHECK(name.size() == 2*DIOS_NAME_BYTES)
        << "Name length wrong: " << name.size() << " hex chars (expected: "
        << 2*DIOS_NAME_BYTES << " chars)";
    uint32_t j = 0;
    for (uint32_t i = 0; i < DIOS_NAME_BYTES; j += 2, ++i) {
      name_dec[i] = (xtod(name.c_str()[j]) << 4 | xtod(name.c_str()[j+1]));
    }
    memcpy(name_.raw, name_dec, DIOS_NAME_BYTES);
  } else {
    // Check length
    CHECK(name.size() == DIOS_NAME_BYTES) << "Name length wrong: "
                                          << name.size() << " bytes (expected: "
                                          << DIOS_NAME_BYTES << " bytes)";
    // Set the name
    memcpy(name_.raw, name.data(), DIOS_NAME_BYTES);
  }
}


bool DataObject::operator==(const DataObject& other_do) const {
  return (memcmp(name_.raw, other_do.name_.raw, DIOS_NAME_BYTES) == 0);
}

bool DataObject::operator<(const DataObject& other_do) const {
  return (memcmp(name_.raw, other_do.name_.raw, DIOS_NAME_BYTES) < 0);
}

DataObject::DataObject(const char* name) {
  memcpy(name_.raw, name, DIOS_NAME_BYTES);
}

DataObject::DataObject(const uint8_t* name) {
  // Set the name
  memcpy(name_.raw, name, DIOS_NAME_BYTES);
}


}  // namespace firmament
