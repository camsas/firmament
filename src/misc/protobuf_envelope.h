// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Specialized transmission envelope for sending protobufs.

#ifndef FIRMAMENT_MISC_PROTOBUF_ENVELOPE_H
#define FIRMAMENT_MISC_PROTOBUF_ENVELOPE_H

#include <vector>

#include "messages/base_message.pb.h"
#include "misc/envelope.h"

namespace firmament {
namespace misc {

// Message envelope.
template <>
class Envelope<BaseMessage> : public PrintableInterface {
 public:
  Envelope() : data_(NULL), is_owner_(false) {}
  explicit Envelope(BaseMessage *data) : data_(data), is_owner_(false) {}
  virtual ~Envelope() {
    VLOG(2) << "Envelope at " << this << " is being destroyed.";
    VLOG(2) << "At destruction, content is: " << *this;
    if (is_owner_)
      // de-allocate buffer
      delete data_;
  }
  virtual size_t size() const {
    CHECK(data_ != NULL) << "Protobuf message envelope has NULL content.";
    return data_->ByteSize();
  }
  virtual BaseMessage* data() {
    CHECK(data_ != NULL) << "Protobuf message envelope has NULL content.";
    // TODO(malte): consider moving this to a shared_ptr
    return data_;
  }
  virtual bool Parse(void *buffer, size_t length) {
    if (!data_) {
      // XXX(malte): check if we can use smart pointers instead here.
      VLOG(2) << "Allocating new message inside envelope at " << this;
      data_ = new BaseMessage();
      is_owner_ = true;
    }
    return data_->ParseFromArray(buffer, length);
  }
  virtual bool Serialize(void *buffer, size_t length) const {
    CHECK(data_ != NULL) << "Tried to serialize a protobuf message envelope "
                         << "with NULL contents.";
    return data_->SerializeToArray(buffer, length);
  }
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "(PB Envelope, size=" << size() << ", at=" << this
                   << ", data=" << data_->DebugString() << ")";
  }

 private:
  // unit tests
  FRIEND_TEST(EnvelopeTest, EmptyParseProtobuf);
  FRIEND_TEST(EnvelopeTest, EmptyParseBlankProtobuf);
  FRIEND_TEST(EnvelopeTest, StashProtobuf);
  // fields
  BaseMessage* data_;
  bool is_owner_;
};

}  // namespace misc
}  // namespace firmament

#endif  // FIRMAMENT_MISC_ENVELOPE_H
