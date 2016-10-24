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

// Data object class definition. In Firmament, a data object is a chunk of
// binary data, in memory or not, that can be read, written or executed. It is
// globally uniquely named by a DataObjectName.
//
// The notion of a Firmament data object is closely related to the DIOS notions
// of a name and a reference.

#ifndef FIRMAMENT_DATA_OBJECT_H
#define FIRMAMENT_DATA_OBJECT_H

#include <cstdio>
#include <string>

#include "base/common.h"
#include "misc/printable_interface.h"

namespace firmament {

// Imported from libDIOS.
#define DIOS_NAME_BITS 256
#define DIOS_NAME_BYTES (DIOS_NAME_BITS/8)
#define DIOS_NAME_QWORDS (DIOS_NAME_BYTES/8)

#define xtod(c) ((c >= '0' && c <= '9') ? c-'0' : ((c >= 'A' && c <= 'F') ? \
                 c-'A'+10 : ((c >= 'a' && c <= 'f') ? c-'a'+10 : 0)))

typedef struct {
  union {
    uint64_t value[DIOS_NAME_QWORDS];
    uint8_t raw[DIOS_NAME_BYTES];
  };
} dios_name_t;

class DataObject : public PrintableInterface {
 public:
  explicit DataObject(const dios_name_t& name);
  explicit DataObject(const char* name);
  explicit DataObject(const uint8_t* name);
  DataObject(const string& name, bool hex_decode);
  bool operator==(const DataObject& other_do) const;
  bool operator<(const DataObject& other_do) const;


  inline const uint8_t* name() const {
    return name_.raw;
  }
  inline const string* name_str() const {
    char ret[DIOS_NAME_BYTES+1];
    memcpy(ret, name_.raw, DIOS_NAME_BYTES);
    ret[DIOS_NAME_BYTES] = '\0';
    return new string(ret);
  }
  inline const char* name_bytes() const {
    return reinterpret_cast<const char*>(name_.raw);
  }
  inline const string name_printable_string() const {
    char c_str[3];
    string ret;
    // TODO(malte): Sadly, this is very inefficient, but I couldn't figure out a
    // better way to do it while keeping the byte order correct.
    for (uint32_t i = 0; i < DIOS_NAME_BYTES; ++i) {
      snprintf(c_str, sizeof(c_str), "%02hhx", name_.raw[i]);
      ret += c_str;
    }
    return ret;
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<DataObject, name=" << name_printable_string() << ", at="
                   << this << ", location=" << ">";
  }

 private:
  dios_name_t name_;
  //dios_value_t location_;
};

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_DATA_OBJECT_H
