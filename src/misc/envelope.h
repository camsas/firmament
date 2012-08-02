// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Generic transmission envelope for messages.

#ifndef FIRMAMENT_MISC_ENVELOPE_H
#define FIRMAMENT_MISC_ENVELOPE_H

#include <string>

#include "base/common.h"
#include "misc/printable_interface.h"

namespace firmament {
namespace misc {

// Message envelope.
template <typename T>
class Envelope : public PrintableInterface {
 public:
  explicit Envelope(T *data) : data_(data) {}
  virtual size_t size() const {
    return sizeof(data_);
  }
  virtual T* data() {
    // TODO(malte): consider moving this to a shared_ptr
    return data_;
  }
  virtual bool Parse(void *buffer, size_t length) const {
    // XXX(malte): Totally unsafe :)
    memcpy(static_cast<void*>(data_), buffer, length);
    return true;
  }
  virtual bool Serialize(void *buffer, size_t length) const {
    // XXX(malte): Totally unsafe :)
    memcpy(buffer, static_cast<void*>(data_), length);
    return true;
  }
  virtual ostream& ToString(ostream& stream) const {
    // TODO(malte): Print the first few bytes of the data here
    return stream << "(Envelope,size=" << size() << ",at=" << this
                  << ",data=)";
  }
 private:
  T *data_;
};

}  // namespace misc
}  // namespace firmament

#endif  // FIRMAMENT_MISC_ENVELOPE_H
