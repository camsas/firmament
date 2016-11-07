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

// Implementation of some common utility methods for Firmament nodes. Note that
// Node is an abstract class, so it cannot be instantiated directly.

#include "engine/node.h"

#include <string>
#include <utility>

#include <sys/types.h>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#endif

#include "base/resource_desc.pb.h"
#include "messages/base_message.pb.h"
#include "misc/protobuf_envelope.h"
#include "misc/map-util.h"
#include "misc/utils.h"

DEFINE_uint64(heartbeat_interval, 1000000,
              "Heartbeat interval in microseconds.");
DEFINE_string(platform, "PL_UNIX", "The platform we are running on, or AUTO "
              "for attempting automatic discovery.");
DEFINE_string(listen_uri, "tcp:localhost:9998",
              "The name/address/port to listen on.");

namespace firmament {

// Initial value of exit_ toggle
bool Node::exit_ = false;

Node::Node(ResourceID_t uuid)
  : node_uri_(FLAGS_listen_uri),
    uuid_(uuid) {
  // Set up the node's resource descriptor
  resource_desc_.set_uuid(to_string(uuid_));

  // Set up a message adapter for control messages.
  m_adapter_ = new platform_unix::streamsockets::
      StreamSocketsAdapter<BaseMessage>();
  VLOG(1) << "Node's adapter is at " << m_adapter_;
  // Also set up a signal handler so that we can quit the node using
  // signals.
  SignalHandler handler;
  handler.ConfigureSignal(SIGINT, Node::HandleSignal, this);
  handler.ConfigureSignal(SIGTERM, Node::HandleSignal, this);
}

Node::~Node() {
}

bool Node::ConnectToRemote(
    const string& remote_uri,
    StreamSocketsChannel<BaseMessage>* chan) {
  return m_adapter_->EstablishChannel(remote_uri, chan);
}

bool Node::SendMessageToRemote(
    StreamSocketsChannel<BaseMessage>* chan,
    BaseMessage* msg) {
  Envelope<BaseMessage> envelope(msg);
  // Must send synchronously as we cannot assume that msg stays around
  // forever!
  return chan->SendS(envelope);
}

void Node::HandleWrite(const boost::system::error_code& error,
                       size_t bytes_transferred) {
  VLOG(3) << "In HandleWrite, thread is " << boost::this_thread::get_id();
  if (error)
    LOG(ERROR) << "Error returned from async write: " << error.message();
  else
    VLOG(3) << "bytes_transferred: " << bytes_transferred;
}

void Node::Run() {
  // Node starting -- set up and wait for others to connect.
  m_adapter_->ListenURI(FLAGS_listen_uri);
  m_adapter_->RegisterAsyncMessageReceiptCallback(
      boost::bind(&Node::HandleIncomingMessage, this, _1, _2));
  m_adapter_->RegisterAsyncErrorPathCallback(
      boost::bind(&Node::HandleIncomingReceiveError, this,
                  boost::asio::placeholders::error, _2));

  // Main loop
  while (!exit_) {
    // Wait for events (i.e. messages from workers.
    // TODO(malte): we need to think about any actions that the coordinator
    // itself might need to take, and how they can be triggered
    VLOG(3) << "Hello from main loop!";
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
  Shutdown("dropped out of main loop");
}

void Node::AwaitNextMessage() {
  VLOG(3) << "Waiting for next message from adapter...";
  m_adapter_->AwaitNextMessage();
  //boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void Node::HandleIncomingReceiveError(
    const boost::system::error_code& error,
    const string& remote_endpoint) {
  // Notify of receive error
  // TODO(malte): since we are taking no arguments, we actually don't have the
  // faintest idea what message this error relates to. We should try to remedy
  // this; however, it is not trivial, since we destroy the channel before
  // making the callback (and we do not want to have the callback on the
  // critical path to unlocking the receive lock, as it may take a long time to
  // run).
  if (error.value() == boost::asio::error::eof) {
    // Connection terminated, handle accordingly
    LOG(INFO) << "Connection to " << remote_endpoint << " closed.";
    // XXX(malte): Need to figure out if this relates to a resource, and if so,
    // if we should declare it failed; or whether this is an expected job
    // completion.
  } else {
    LOG(WARNING) << "Failed to complete a message receive cycle from "
                 << remote_endpoint << ". The message was discarded, or the "
                 << "connection failed (error: " << error.message() << ", "
                 << "code " << error.value() << ").";
  }
}

void Node::HandleSignal(int signum) {
  // TODO(malte): handle other signals, and do not necessarily quit.
  VLOG(1) << "Received signal " << signum << ", terminating...";
  if (signum == SIGTERM || signum == SIGINT)
    exit_ = true;
}

void Node::Shutdown(const string& reason) {
  LOG(INFO) << "Node shutting down; reason: " << reason;
  m_adapter_->StopListen();
  VLOG(1) << "All connections shut down; now exiting...";
  // Toggling the exit flag will make the Node drop out of its main loop.
  exit_ = true;
}

}  // namespace firmament
