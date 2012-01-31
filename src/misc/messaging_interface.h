// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Messaging interface definition.

#ifndef FIRMAMENT_MISC_MESSAGING_INTERFACE_H
#define FIRMAMENT_MISC_MESSAGING_INTERFACE_H

#include "base/common.h"
#include "platforms/common.h"

namespace firmament {

typedef ::google::protobuf::Message Message;

template <class T>
class MessagingChannelInterface {
 public:
  // Send (sync?)
  virtual void Send(const T& message) = 0;
  // Synchronous receive -- blocks until the next message is received.
  virtual T* RecvS() = 0;
  // Asynchronous receive -- does not block.
  virtual T* RecvA() = 0;
};

class MessagingInterface {
 public:
  template <class T>
  void EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan);
  // TODO(malte): Do we actually want to do this, or should we leave it to the
  // system to close channels when no longer needed? Alternative might be a
  // DetachChannel call, which leaves the channel open for potential reuse, but
  // invalidates the reference to it.
  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  virtual Message* AwaitNextMessage() = 0;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_MESSAGING_INTERFACE_H
