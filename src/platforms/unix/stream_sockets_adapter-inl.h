// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter; inline header for templated
// methods.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H

#include "misc/map-util.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

#include <string>
#include <utility>

namespace firmament {
namespace platform_unix {
namespace streamsockets {

template <typename T>
StreamSocketsAdapter<T>::StreamSocketsAdapter()
  : message_recv_handler_(NULL),
    error_path_handler_(NULL),
    message_wait_ready_(false) {
}

template <typename T>
void StreamSocketsAdapter<T>::CloseChannel(
    MessagingChannelInterface<T>* chan) {
  VLOG(1) << "Shutting down channel " << chan;
  chan->Close();
}

template <typename T>
bool StreamSocketsAdapter<T>::EstablishChannel(
    const string& endpoint_uri,
    MessagingChannelInterface<T>* chan) {
  return _EstablishChannel(endpoint_uri,
                           static_cast<StreamSocketsChannel<T>*>(chan));
}

template <typename T>
bool StreamSocketsAdapter<T>::_EstablishChannel(
    const string& endpoint_uri,
    StreamSocketsChannel<T>* chan) {
  VLOG(1) << "Establishing channel to endpoint " << endpoint_uri
          << ", chan: " << *chan << "!";
  bool result = chan->Establish(endpoint_uri);
  boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
  InsertIfNotPresent(&endpoint_channel_map_, endpoint_uri, chan);
  return result;
}

template <typename T>
StreamSocketsAdapter<T>::~StreamSocketsAdapter() {
  VLOG(2) << "Messaging adapter is being destroyed.";
  StopListen();
}

template <typename T>
void StreamSocketsAdapter<T>::AddChannelForConnection(
    TCPConnection::connection_ptr connection) {
  StreamSocketsChannel<T>* channel =
          new StreamSocketsChannel<T>(connection);
  const string endpoint_name = connection->RemoteEndpointString();
  VLOG(1) << "Adding back-channel for connection at " << connection
          << ", channel is " << *channel << ", remote endpoint: "
          << endpoint_name;
  boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
  InsertIfNotPresent(&endpoint_channel_map_, endpoint_name, channel);
  if (VLOG_IS_ON(2))
    DumpActiveChannels();
  // Unblock any waiters, since there's now an additional connection
  message_wait_ready_ = true;
  message_wait_condvar_.notify_all();
}

template <typename T>
MessagingChannelInterface<T>* StreamSocketsAdapter<T>::GetChannelForEndpoint(
    const string& endpoint) {
  CHECK_NE(endpoint, "");
  boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
  StreamSocketsChannel<T>** chan =
      FindOrNull(endpoint_channel_map_, endpoint);
  if (VLOG_IS_ON(3))
    DumpActiveChannels();
  if (!chan)
    // No channel found
    return NULL;
  // Return channel pointer
  return *chan;
}

template <typename T>
void StreamSocketsAdapter<T>::AwaitNextMessage() {
  VLOG(3) << "Iterating over " << endpoint_channel_map_.size()
          << " active channels in adapter " << this;
  if (VLOG_IS_ON(3))
    DumpActiveChannels();
  // If we have no active channels, we cannot receive any messages, so we return
  // immediately.
  if (endpoint_channel_map_.size() == 0)
    return;
  // Otherwise, let's make sure we have an outstanding async receive request for
  // each fo them.
  bool any_outstanding = false;
  {
    boost::lock_guard<boost::mutex> lock(endpoint_channel_map_mutex_);
    boost::lock_guard<boost::mutex> envel_lock(channel_recv_envelopes_mutex_);
    for (__typeof__(endpoint_channel_map_.begin()) chan_iter =
         endpoint_channel_map_.begin();
         chan_iter != endpoint_channel_map_.end();
         ++chan_iter) {
      StreamSocketsChannel<T>* chan = chan_iter->second;
      if (!channel_recv_envelopes_.count(chan)) {
        // No outstanding receive request for this channel, so create one
        Envelope<T>* envelope = new Envelope<T>();
        channel_recv_envelopes_.insert(
            pair<StreamSocketsChannel<T>*,
            Envelope<T>*>(chan, envelope));
        VLOG(2) << "MA replenishing envelope for channel " << chan
                << " at " << envelope;
        chan->RecvA(envelope,
                    boost::bind(&StreamSocketsAdapter::HandleAsyncMessageRecv,
                                this,
                                boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred,
                                chan));
        any_outstanding = true;
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

template <typename T>
void StreamSocketsAdapter<T>::HandleAsyncMessageRecv(
    const boost::system::error_code& error,
    size_t bytes_transferred,
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
      boost::lock_guard<boost::mutex> envel_lock(channel_recv_envelopes_mutex_);
      CHECK(endpoint_channel_map_.erase(remote_endpoint));
      CHECK(channel_recv_envelopes_.erase(chan));
    } else {
      LOG(ERROR) << "Failed to receive on channel at " << chan << ", which no "
                 << "longer has an endpoint set. Cannot remove from "
                 << "endpoint/channel map!";
    }
    // After we have removed the channel from the map of active channels and
    // deleted its next receive envelope, we can close it without danger.
    chan->Close();
    // Finally, we also need to ask the TCP server to remove the connection from
    // its map of connections
    tcp_server_->DropConnectionForEndpoint(remote_endpoint);
    // At this point, we unlock and signal to the condition variable, so that we
    // can pick another channel to receive on, or go back to the idle loop (if
    // no other channels are available).
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
  CHECK(channel_recv_envelopes_.count(chan));
  Envelope<T>* envelope = channel_recv_envelopes_[chan];
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
  // this is not enforced. In fact, the below may work in that it gives us what
  // is essentially broadcast semantics if multiple threads are waiting.
  message_wait_condvar_.notify_all();
}

template <typename T>
void StreamSocketsAdapter<T>::ListenURI(const string& endpoint_uri) {
  // Extract hostname and port from URI
  string hostname = URITools::GetHostnameFromURI(endpoint_uri);
  string port = URITools::GetPortFromURI(endpoint_uri);
  Listen(hostname, port);
}

template <typename T>
void StreamSocketsAdapter<T>::Listen(const string& hostname,
                                     const string& port) {
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
          &StreamSocketsAdapter::AddChannelForConnection,
          this, _1)));
  VLOG(2) << "TCP server created";
  tcp_server_thread_.reset(
      new boost::thread(boost::bind(&AsyncTCPServer::Run, tcp_server_)));
  VLOG(1) << "AsyncTCPServer's main thread running as "
          << tcp_server_thread_->get_id();
}

template <typename T>
string StreamSocketsAdapter<T>::Listen(const string& hostname) {
  // no-op if we are already listening
  /*if (ListenReady())
    return;*/
  CHECK(!ListenReady());
  CHECK_EQ(endpoint_channel_map_.size(), 0);
  CHECK_EQ(channel_recv_envelopes_.size(), 0);
  message_wait_mutex_.lock();
  message_wait_ready_ = false;
  message_wait_mutex_.unlock();
  // Unknown port, but known hostname
  VLOG(1) << "Creating an async TCP server with an unspecified port ";
  tcp_server_.reset(new AsyncTCPServer(
      hostname, "", boost::bind(
          &StreamSocketsAdapter::AddChannelForConnection,
          this, _1)));
  VLOG(2) << "TCP server created";
  tcp_server_thread_.reset(
      new boost::thread(boost::bind(&AsyncTCPServer::Run, tcp_server_)));
  VLOG(1) << "AsyncTCPServer's main thread running as "
          << tcp_server_thread_->get_id();
  return tcp_server_->listening_interface();
}



template <typename T>
bool StreamSocketsAdapter<T>::ListenReady() {
  if (tcp_server_)
    return tcp_server_->listening();
  else
    return false;
}

template <typename T>
void StreamSocketsAdapter<T>::RegisterAsyncMessageReceiptCallback(
    typename AsyncMessageRecvHandler<T>::type callback) {
  message_recv_handler_ = callback;
}

template <typename T>
void StreamSocketsAdapter<T>::RegisterAsyncErrorPathCallback(
    typename AsyncErrorPathHandler<T>::type callback) {
  error_path_handler_ = callback;
}

template <typename T>
bool StreamSocketsAdapter<T>::SendMessageToEndpoint(
    const string& endpoint_uri, T& message) {
  StreamSocketsChannel<T>** chan =
      FindOrNull(endpoint_channel_map_, endpoint_uri);
  if (!chan) {
    LOG(ERROR) << "Failed to find channel for endpoint " << endpoint_uri;
    return false;
  }
  // N.B.: Synchronous send here means that it's okay to stack-allocate the
  // Envelope; if we ever switch to async or provide such a facility, this needs
  // to be dynamically allocated.
  Envelope<T> envelope(&message);
  return (*chan)->SendS(envelope);
}

template <typename T>
void StreamSocketsAdapter<T>::StopListen() {
  if (tcp_server_) {
    for (__typeof__(endpoint_channel_map_.begin()) chan_iter =
         endpoint_channel_map_.begin();
         chan_iter != endpoint_channel_map_.end();
         ++chan_iter) {
      VLOG(2) << "Closing associated channel at " << chan_iter->second;
      chan_iter->second->Close();
    }
    VLOG(2) << "Stopping async TCP server at " << tcp_server_
            << "...";
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

template <class T>
ostream& StreamSocketsAdapter<T>::ToString(ostream* stream) const {
  return *stream << "(MessagingAdapter,type=StreamSockets,at=" << this
                 << ",num_channels=" << endpoint_channel_map_.size() << ")";
}


}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
