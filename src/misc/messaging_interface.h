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

// Messaging interface definition.

#ifndef FIRMAMENT_MISC_MESSAGING_INTERFACE_H
#define FIRMAMENT_MISC_MESSAGING_INTERFACE_H

#include <string>

#include "base/common.h"
#include "misc/envelope.h"
#include "misc/printable_interface.h"
#include "platforms/unix/common.h"

namespace firmament {

using platform_unix::AsyncSendHandler;
using platform_unix::AsyncRecvHandler;
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
  // Check if the channel is ready to send/receive
  virtual bool Ready() = 0;
  // Debug output generating method.
  virtual ostream& ToString(ostream* stream) const = 0;
  // Return a string representing the endpoint at the local end of the channel.
  // This is the local end of the underlying connection it wraps, specified in
  // the form: <protocol>://<address[:port]>
  virtual const string LocalEndpointString() = 0;
  // Return a string representing the endpoint at the remote end of the channel.
  // This is the remote end of the underlying connection it wraps, specified in
  // the form: <protocol>://<address[:port]>
  virtual const string RemoteEndpointString() = 0;
};

template <typename T>
class MessagingAdapterInterface : public PrintableInterface {
 public:
  // Blocking wait for a new message to arrive.
  virtual void AwaitNextMessage() = 0;
  // TODO(malte): Do we actually want to do this, or should we leave it to the
  // system to close channels when no longer needed? Alternative might be a
  // DetachChannel call, which leaves the channel open for potential reuse, but
  // invalidates the reference to it.
  virtual void CloseChannel(MessagingChannelInterface<T>* chan) = 0;
  // Returns the channel corresponding to an endpoint specifier string, or a
  // NULL pointer if no channel to this endpoint is held by the adapter.
  virtual MessagingChannelInterface<T>*
      GetChannelForEndpoint(const string& endpoint) = 0;
  // Set up a messaging channel to a remote endpoint.
  virtual bool EstablishChannel(
      const string& endpoint_uri,
      MessagingChannelInterface<T>* chan) = 0;
  // Listen for incoming channel establishment requests.
  virtual void ListenURI(const string& endpoint_uri) = 0;
  // Check if we are ready to accept connections.
  virtual bool ListenReady() = 0;
  // Start asynchronous receive loop
  //virtual void StartAsyncRecvLoop() = 0;
  // Register message receipt callback
  virtual void RegisterAsyncMessageReceiptCallback(
      typename AsyncMessageRecvHandler<T>::type callback) = 0;
  // Register error callback
  virtual void RegisterAsyncErrorPathCallback(
      typename AsyncErrorPathHandler<T>::type callback) = 0;
  // Send a message to a remote endpoint. If the resource is not associated
  // (i.e. there is no channel to it, or it has failed), the boolean return
  // value will be false.
  virtual bool SendMessageToEndpoint(const string& endpoint_uri,
                                     T& message) = 0;  // NOLINT
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_MESSAGING_INTERFACE_H
