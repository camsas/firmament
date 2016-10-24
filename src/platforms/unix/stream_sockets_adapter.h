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

// UNIX stream sockets-based messaging adapter.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H

#include <string>
#include <vector>
#include <map>
#include <set>

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

// This needs to go above other includes as it defines __PLATFORM_UNIX__
#include "platforms/unix/common.h"

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "misc/map-util.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"
#include "platforms/unix/stream_sockets_channel.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Messaging adapter.
template <typename T>
class StreamSocketsAdapter : public firmament::MessagingAdapterInterface<T>,
  public boost::enable_shared_from_this<StreamSocketsAdapter<T> >,
  private boost::noncopyable {
 public:
  StreamSocketsAdapter() : message_recv_handler_(NULL),
    error_path_handler_(NULL),
    message_wait_ready_(false) {
  }

  virtual ~StreamSocketsAdapter() {
    VLOG(2) << "Messaging adapter is being destroyed.";
    StopListen();
  }

  void AwaitNextMessage() {
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    VLOG(3) << "Iterating over " << endpoint_channel_map_.size()
            << " active channels in adapter " << this;
    if (VLOG_IS_ON(3))
      DumpActiveChannels();
    // If we have no active channels, we cannot receive any messages, so we
    // return immediately.
    if (endpoint_channel_map_.size() == 0)
      return;
    // Otherwise, let's make sure we have an outstanding async receive request
    // for each fo them.
    {
      boost::lock_guard<boost::mutex> envel_lock(channel_recv_envelopes_mutex_);
      for (__typeof__(endpoint_channel_map_.begin()) chan_iter =
             endpoint_channel_map_.begin();
           chan_iter != endpoint_channel_map_.end();
           ++chan_iter) {
        StreamSocketsChannel<T>* chan = chan_iter->second;
        if (!channel_recv_envelopes_.count(chan)) {
          // No outstanding receive request for this channel, so create one
          Envelope<T>* envelope = new Envelope<T>();
          CHECK(InsertIfNotPresent(&channel_recv_envelopes_, chan, envelope));
          VLOG(2) << "MA replenishing envelope for channel " << chan
                  << " at " << envelope;
          chan->RecvA(envelope,
                      boost::bind(&StreamSocketsAdapter::HandleAsyncMessageRecv,
                                  this,
                                  boost::asio::placeholders::error,
                                  boost::asio::placeholders::bytes_transferred,
                                  chan));
        }
      }
    }
    /*if (any_outstanding) {
    // Block until we receive a message somewhere
    VLOG(3) << "About to lock mutex...";
    boost::unique_lock<boost::mutex> lock(message_wait_mutex_);
    VLOG(3) << "Locked!...";
    while (!message_wait_ready_) {
    VLOG(3) << "Waiting for condvar...";
    message_wait_condvar_.wait(lock);
    }
    VLOG(3) << "Message arrived, condvar signalled!";
    message_wait_ready_ = false;
    }*/
  }

  void AddChannelForConnection(TCPConnection::connection_ptr connection) {
    StreamSocketsChannel<T>* channel =
      new StreamSocketsChannel<T>(connection);
    const string endpoint_name = connection->RemoteEndpointString();
    VLOG(1) << "Adding back-channel for connection at " << connection
            << ", channel is " << *channel << ", remote endpoint: "
            << endpoint_name;
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    InsertIfNotPresent(&endpoint_channel_map_, endpoint_name, channel);
    if (VLOG_IS_ON(3))
      DumpActiveChannels();
    // Unblock any waiters, since there's now an additional connection
    message_wait_ready_ = true;
    message_wait_condvar_.notify_all();
  }

  void CloseChannel(MessagingChannelInterface<T>* channel) {
    VLOG(1) << "Shutting down channel " << channel;
    channel->Close();
  }

  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* channel) {
    return _EstablishChannel(endpoint_uri,
                             static_cast<StreamSocketsChannel<T>*>(channel));
  }

  MessagingChannelInterface<T>* GetChannelForEndpoint(const string& endpoint) {
    CHECK_NE(endpoint, "");
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    StreamSocketsChannel<T>* channel =
      FindPtrOrNull(endpoint_channel_map_, endpoint);
    if (VLOG_IS_ON(3))
      DumpActiveChannels();
    return channel;
  }

  void ListenURI(const string& endpoint_uri) {
    string hostname = URITools::GetHostnameFromURI(endpoint_uri);
    string port = URITools::GetPortFromURI(endpoint_uri);
    Listen(hostname, port);
  }

  void Listen(const string& hostname, const string& port) {
    // no-op if we are already listening
    /*if (ListenReady())
      return;*/
    CHECK(!ListenReady());
    CHECK_EQ(endpoint_channel_map_.size(), 0);
    CHECK_EQ(channel_recv_envelopes_.size(), 0);
    message_wait_mutex_.lock();
    message_wait_ready_ = false;
    message_wait_mutex_.unlock();
    VLOG(1) << "Creating an async TCP server on port " << port
            << " on endpoint " << hostname;
    tcp_server_.reset(new AsyncTCPServer(
        hostname, port, boost::bind(
            &StreamSocketsAdapter::AddChannelForConnection, this, _1)));
    CHECK(tcp_server_);
    VLOG(2) << "TCP server created";
    tcp_server_thread_.reset(
        new boost::thread(boost::bind(&AsyncTCPServer::Run, tcp_server_)));
    VLOG(1) << "AsyncTCPServer's main thread running as "
            << tcp_server_thread_->get_id();
  }

  string Listen(const string& hostname) {
    // known hostname, but unknown port
    Listen(hostname, "");
    return tcp_server_->listening_interface();
  }

  bool ListenReady() {
    if (tcp_server_)
      return tcp_server_->listening();
    else
      return false;
  }

  void RegisterAsyncMessageReceiptCallback(
      typename AsyncMessageRecvHandler<T>::type callback) {
    message_recv_handler_ = callback;
  }

  void RegisterAsyncErrorPathCallback(
      typename AsyncErrorPathHandler<T>::type callback) {
    error_path_handler_ = callback;
  }

  bool SendMessageToEndpoint(const string& endpoint_uri, T& message) {  // NOLINT
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    StreamSocketsChannel<T>* channel =
      FindPtrOrNull(endpoint_channel_map_, endpoint_uri);
    if (!channel) {
      LOG(ERROR) << "Failed to find channel for endpoint " << endpoint_uri;
      return false;
    }
    // N.B.: Synchronous send here means that it's okay to stack-allocate the
    // Envelope; if we ever switch to async or provide such a facility, this
    // needs to be dynamically allocated.
    Envelope<T> envelope(&message);
    return channel->SendS(envelope);
  }

  void StopListen() {
    if (tcp_server_) {
      for (__typeof__(endpoint_channel_map_.begin()) chan_iter =
             endpoint_channel_map_.begin();
           chan_iter != endpoint_channel_map_.end();
           ++chan_iter) {
        VLOG(2) << "Closing associated channel at " << chan_iter->second;
        chan_iter->second->Close();
      }
      VLOG(2) << "Stopping async TCP server at " << tcp_server_ << "...";
      CHECK(tcp_server_);
      tcp_server_->Stop();
      tcp_server_thread_->join();
      VLOG(2) << "TCP server thread joined.";
    }
    message_wait_condvar_.notify_all();
    // XXX(malte): We would prefer if channels cleared up after themselves, but
    // for the moment, this is a sledgehammer approach.
    VLOG(1) << "Dropping channels and outstanding requests...";
    endpoint_channel_map_.clear();
    channel_recv_envelopes_.clear();
  }

  ostream& ToString(ostream* stream) const {
    return *stream << "(MessagingAdapter,type=StreamSockets,at=" << this
                   << ",num_channels=" << endpoint_channel_map_.size() << ")";
  }

  uint32_t NumActiveChannels() {
    return endpoint_channel_map_.size();
  }
  void DumpActiveChannels() {
    LOG(INFO) << NumActiveChannels() << " active channels at " << *this;
    for (__typeof__(endpoint_channel_map_.begin()) chan_iter =
         endpoint_channel_map_.begin();
         chan_iter != endpoint_channel_map_.end();
         ++chan_iter) {
      LOG(INFO) << "[" << chan_iter->second << "]: Local: "
                << chan_iter->second->LocalEndpointString()
                << " Remote: " << chan_iter->second->RemoteEndpointString();
    }
  }

 private:
  void HandleAsyncMessageRecv(
      const boost::system::error_code& error,
      uint64_t bytes_transferred,
      StreamSocketsChannel<T>* chan) {
    if (error) {
      VLOG(1) << "Failed to receive message on MA " << *this;
      // TODO(malte): think about clearing up state here. Should we consider the
      // envelope as having been consumed? Currently we do so.
      // XXX(malte): the below is possibly unsafe in some way, especially w.r.t.
      // concurrency
      string remote_endpoint = chan->RemoteEndpointString();
      if (remote_endpoint != "") {
        boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
        boost::lock_guard<boost::mutex> envel_lock(
            channel_recv_envelopes_mutex_);
        CHECK(endpoint_channel_map_.erase(remote_endpoint));
        CHECK(channel_recv_envelopes_.erase(chan));
      } else {
        LOG(ERROR) << "Failed to receive on channel at " << chan
                   << ", which no longer has an endpoint set. Cannot remove "
                   << "from endpoint/channel map!";
      }
      // After we have removed the channel from the map of active channels and
      // deleted its next receive envelope, we can close it without danger.
      chan->Close();
      // Finally, we also need to ask the TCP server to remove the connection
      // from its map of connections
      if (tcp_server_)
        tcp_server_->DropConnectionForEndpoint(remote_endpoint);
      // At this point, we unlock and signal to the condition variable, so that
      // we can pick another channel to receive on, or go back to the idle loop
      // (if no other channels are available).
      message_wait_ready_ = true;
      message_wait_condvar_.notify_all();
      // Invoke error callback, if any registered
      if (error_path_handler_)
        error_path_handler_(error, remote_endpoint);
      else
        LOG(ERROR) << "Unhandled error condition for failed receive from "
                   << remote_endpoint;
      return;
    }
    CHECK_GT(channel_recv_envelopes_.count(chan), 0)
      << "No envelopes around when we expected to have at least one.";
    Envelope<T>* envelope = FindPtrOrNull(channel_recv_envelopes_, chan);
    CHECK_NOTNULL(envelope);
    VLOG(2) << "Received in MA: " << *envelope << " ("
            << bytes_transferred << ")";
    // Invoke message receipt callback, if any registered
    CHECK(message_recv_handler_ != NULL);
    message_recv_handler_(envelope->data(), chan->RemoteEndpointString());
    // We've finished dealing with this message, so clean up now.
    {
      boost::lock_guard<boost::mutex> lock(channel_recv_envelopes_mutex_);
      channel_recv_envelopes_.erase(chan);
      delete envelope;
    }
    {
      boost::lock_guard<boost::mutex> lock(message_wait_mutex_);
      message_wait_ready_ = true;
    }
    // TODO(malte): Not sure if we want notify_all here. So far, we assume that
    // there is only one call to AwaitNextMessage() processing at any time; but
    // this is not enforced. In fact, the below may work in that it gives us
    // what is essentially broadcast semantics if multiple threads are waiting.
    message_wait_condvar_.notify_all();
  }

  bool _EstablishChannel(const string& endpoint_uri,
                         StreamSocketsChannel<T>* chan) {
    VLOG(1) << "Establishing channel to endpoint " << endpoint_uri
            << ", chan: " << *chan << "!";
    bool result = chan->Establish(endpoint_uri);
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    InsertIfNotPresent(&endpoint_channel_map_, endpoint_uri, chan);
    return result;
  }

  typename AsyncMessageRecvHandler<T>::type message_recv_handler_;
  typename AsyncErrorPathHandler<T>::type error_path_handler_;
  shared_ptr<AsyncTCPServer> tcp_server_;
  scoped_ptr<boost::thread> tcp_server_thread_;
  //set<shared_ptr<StreamSocketsChannel<T> > > active_channels_;
  unordered_map<string, StreamSocketsChannel<T>*> endpoint_channel_map_;
  unordered_map<StreamSocketsChannel<T>*, Envelope<T>*> channel_recv_envelopes_;
  // Synchronization variables, locks tec.
  boost::mutex message_wait_mutex_;
  boost::condition_variable message_wait_condvar_;
  boost::mutex channel_recv_envelopes_mutex_;
  boost::mutex endpoint_channel_map_mutex_;
  bool message_wait_ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
