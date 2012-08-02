// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter; inline header for templated
// methods.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H

#include "platforms/unix/messaging_streamsockets.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

template <class T>
void StreamSocketsMessaging::CloseChannel(
    MessagingChannelInterface<T>* chan) {
  VLOG(1) << "Shutting down channel " << chan;
  chan->Close();
}
template <class T>
bool StreamSocketsMessaging::EstablishChannel(
    const string& endpoint_uri,
    MessagingChannelInterface<T>* chan) {
  VLOG(1) << "Establishing channel from endpoint " << endpoint_uri
          << ", chan: " << *chan << "!";
  return chan->Establish(endpoint_uri);
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
