// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
