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

// Printable interface, defining the operator overload to allow any class
// implementing it to be used as part of << output constructs.

#ifndef FIRMAMENT_MISC_PRINTABLE_INTERFACE_H
#define FIRMAMENT_MISC_PRINTABLE_INTERFACE_H

#include "base/common.h"

namespace firmament {

class PrintableInterface {
 public:
  virtual ~PrintableInterface() {}
  virtual ostream& ToString(ostream* stream) const = 0;
};

template <typename CharT, typename Traits>
basic_ostream<CharT, Traits>& operator<<(
    basic_ostream<CharT, Traits>& stream,
    const PrintableInterface& p) {
  return p.ToString(&stream);
}

}  // namespace firmament

#endif  // FIRMAMENT_MISC_PRINTABLE_INTERFACE_H
