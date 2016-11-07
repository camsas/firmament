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

// General node class. This provides implementations of shared utility methods,
// such as those used for connecting to and communicating with remote nodes.

#ifndef FIRMAMENT_ENGINE_NODE_H
#define FIRMAMENT_ENGINE_NODE_H

#include <string>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/uuid/uuid.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build node!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "platforms/unix/signal_handler.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"

namespace firmament {

using platform_unix::SignalHandler;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;

class Node {
 public:
  explicit Node(ResourceID_t uuid);
  virtual ~Node();
  virtual void Run() = 0;
  void Shutdown(const string& reason);

  inline ResourceID_t uuid() { return uuid_; }

 protected:
  void AwaitNextMessage();
  bool ConnectToRemote(const string& remote_uri,
                       StreamSocketsChannel<BaseMessage>* chan);
  bool SendMessageToRemote(
      StreamSocketsChannel<BaseMessage>* chan,
      BaseMessage* msg);
  virtual void HandleIncomingMessage(BaseMessage *bm,
                                     const string& remote_endpoint) = 0;
  void HandleIncomingReceiveError(const boost::system::error_code& error,
                                  const string& remote_endpoint);
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
#if (BOOST_VERSION < 104700)
  // compatible with C-style signal handler setup
  static void HandleSignal(int signum);
#else
  // Boost ASIO signal handler setup
  static void HandleSignal(int signum);
#endif

  // Boolean flag that when set to true will cause the node to shut down.
  static bool exit_;
  // The configured URI at which this node is reachable.
  string node_uri_;
  // A messaging adapter for the exchange of control messages.
  // TODO(malte): This is specialized to BaseMessage protobufs here. Maybe it
  // should not be?
  StreamSocketsAdapter<BaseMessage>* m_adapter_;
  // This node's own resource descriptor.
  ResourceDescriptor resource_desc_;
  // The node's resource UUID.
  ResourceID_t uuid_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_NODE_H
