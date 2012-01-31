// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"

namespace firmament {

class StreamSocketsMessaging : public MessagingInterface {
 public:
  StreamSocketsMessaging() {}
  template <class T>
  void EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan) {
    VLOG(1) << "got here, endpoint is " << endpoint_uri << ", chan: " << chan
            << "!";
  }

  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  Message* AwaitNextMessage();
};

template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T> {
 public:
  // Send (sync?)
  void Send(const T& message);
  // Synchronous receive -- blocks until the next message is received.
  T* RecvS();
  // Asynchronous receive -- does not block.
  T* RecvA();
};

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
