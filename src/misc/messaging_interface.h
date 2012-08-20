// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Messaging interface definition.

#ifndef FIRMAMENT_MISC_MESSAGING_INTERFACE_H
#define FIRMAMENT_MISC_MESSAGING_INTERFACE_H

#include <string>

#include "base/common.h"
#include "misc/envelope.h"
#include "misc/printable_interface.h"

namespace firmament {

#ifdef __PLATFORM_UNIX__
#include "platforms/unix/common.h"
  /*typedef platform_unix::AsyncSendHandler<T>::type GenericAsyncSendHandler;
  typedef platform_unix::AsyncRecvHandler<T>::type GenericAsyncRecvHandler;*/
using platform_unix::AsyncSendHandler;
using platform_unix::AsyncRecvHandler;
#else
  typedef void(*AsyncSendHandler)(void*, void*);
  typedef void(*AsyncRecvHandler)(void*, void*);
#endif

using firmament::misc::Envelope;

template <typename T>
class MessagingChannelInterface : public PrintableInterface {
 public:
  // Establish a communication channel.
  virtual bool Establish(const string& endpoint_uri) = 0;
  // Send (synchronous)
  virtual bool SendS(const Envelope<T>& message) = 0;
  // Send (asynchronous)
  virtual bool SendA(const Envelope<T>& message,
                     typename AsyncSendHandler<T>::type callback) = 0;
  // Synchronous receive -- blocks until the next message is received.
  virtual bool RecvS(Envelope<T>* message) = 0;
  // Asynchronous receive -- does not block.
  virtual bool RecvA(Envelope<T>* message,
                     typename AsyncRecvHandler<T>::type callback) = 0;
  // Tear down the channel.
  virtual void Close() = 0;
  // Debug output generating method.
  virtual ostream& ToString(ostream* stream) const = 0;
};

class MessagingInterface {
 public:
  // Virtual destructor (mandated by having virtual methods).
  virtual ~MessagingInterface() {}
  // Set up a messaging channel to a remote endpoint.
  template <class T>
  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan);
  // TODO(malte): Do we actually want to do this, or should we leave it to the
  // system to close channels when no longer needed? Alternative might be a
  // DetachChannel call, which leaves the channel open for potential reuse, but
  // invalidates the reference to it.
  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  // Blocking wait for a new message to arrive.
  template <typename T>
  void AwaitNextMessage(Envelope<T>* envelope);
  // Listen for incoming channel establishment requests.
  virtual void Listen(const string& endpoint_uri) = 0;
  // Check if we are ready to accept connections.
  virtual bool ListenReady() = 0;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_MESSAGING_INTERFACE_H
