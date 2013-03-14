// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
// XXX(malte): include order dependency
#include "platforms/unix/common.h"
#include "platforms/unix/signal_handler.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_adapter-inl.h"

namespace firmament {

using platform_unix::SignalHandler;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;

class Node {
 public:
  Node(PlatformID platform_id, ResourceID_t uuid);
  virtual ~Node();
  virtual void Run() = 0;
  void Shutdown(const string& reason);

  inline PlatformID platform_id() {
    return platform_id_;
  }
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
  void HandleRecv(const boost::system::error_code& error,
                  size_t bytes_transferred,
                  Envelope<BaseMessage>* env);
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
#if (BOOST_VERSION < 104700)
  // compatible with C-style signal handler setup
  static void HandleSignal(int signum);
#else
  // Boost ASIO signal handler setup
  static void HandleSignal(int signum);
#endif

  // The node's platform's ID.
  // TODO(malte): This is deprecated and may be removed in the future.
  PlatformID platform_id_;
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
