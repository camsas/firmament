// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Specialized transmission envelope for sending protobufs.

#ifndef FIRMAMENT_MISC_PROTOBUF_ENVELOPE_H
#define FIRMAMENT_MISC_PROTOBUF_ENVELOPE_H

#include "misc/envelope.h"

namespace firmament {
namespace misc {

typedef ::google::protobuf::Message Message;

// Message envelope.
template <>
class Envelope<Message> {
 public:
  explicit Envelope(Message *data) : data_(data) {}
  virtual size_t size() const {
    return data_->ByteSize();
  }
  virtual bool Parse(void *buffer, size_t length) const {
    return data_->ParseFromArray(buffer, length);
  }
  virtual bool Serialize(void *buffer, size_t length) const {
    return data_->SerializeToArray(buffer, length);
  }
 private:
  Message* data_;
};

}  // namespace misc
}  // namespace firmament

#endif  // FIRMAMENT_MISC_ENVELOPE_H
