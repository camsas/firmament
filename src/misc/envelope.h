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
  Envelope() : data_(NULL), is_owner_(false) {}
  explicit Envelope(T *data) : data_(data), is_owner_(false) {}
  virtual ~Envelope() {
    if (is_owner_)
      // de-allocate buffer
      delete data_;
  }
  virtual size_t size() const {
    if (!data_)
      return 0;
    else
      return sizeof(data_);
  }
  virtual T* data() {
    CHECK(data_ != NULL) << "Message envelope has NULL content.";
    // TODO(malte): consider moving this to a shared_ptr
    return data_;
  }
  // Parses a message from a given buffer into the envelopes internal buffer.
  // Since this is the most basic envelope implementation, we simply copy the
  // binary data over.
  // Will allocate memory as required.
  // Returns boolean indication if parse/copy succeeded.
  virtual bool Parse(void *buffer, size_t length) {
    if (!data_) {
      // XXX(malte): check if we can use smart pointers instead here.
      data_ = new T();
      is_owner_ = true;
    }
    // XXX(malte): Totally unsafe :)
    memcpy(static_cast<void*>(data_), buffer, length);
    return true;
  }
  // Serializes the data contained in this envelope into a given buffer.
  // Since this is the most basic envelope implementation, we simply copy the
  // binary data over from the internal buffer to the buffer given as an
  // argument.
  // Will fail if no internal buffer is set.
  // Returns boolean indication if parse/copy succeeded.
  virtual bool Serialize(void *buffer, size_t length) const {
    CHECK(data_ != NULL) << "Tried to serialize a message envelope with "
                         << "NULL contents.";
    // XXX(malte): Totally unsafe :)
    memcpy(buffer, static_cast<void*>(data_), length);
    return true;
  }
  virtual ostream& ToString(ostream* stream) const {
    // TODO(malte): Print only the first few bytes of the data here
    return *stream << "(Envelope: size=" << size() << ", at=" << this
                   << ", data=" << (data_ ? to_string(*data_) : "NULL") << ")";
  }

 private:
  // unit tests
  FRIEND_TEST(EnvelopeTest, EmptyParseInteger);
  FRIEND_TEST(EnvelopeTest, StashInteger);
  // fields
  T *data_;
  bool is_owner_;
};

}  // namespace misc
}  // namespace firmament

#endif  // FIRMAMENT_MISC_ENVELOPE_H
