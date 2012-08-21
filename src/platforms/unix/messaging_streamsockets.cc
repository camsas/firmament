// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of streaming sockets-based messaging adapter.

#include "platforms/unix/messaging_streamsockets.h"

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "platforms/unix/stream_sockets_channel-inl.h"

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// ----------------------------
// StreamSocketsMessaging
// ----------------------------

StreamSocketsMessaging::~StreamSocketsMessaging() {
}

void StreamSocketsMessaging::AddChannelForConnection(
    TCPConnection::connection_ptr connection) {
  shared_ptr<StreamSocketsChannel<BaseMessage> > channel(
          new StreamSocketsChannel<BaseMessage>(connection));
  VLOG(1) << "Adding back-channel for connection at " << connection
          << ", channel is " << *channel;
  active_channels_.push_back(channel);
}

shared_ptr<StreamSocketsChannel<BaseMessage> >
StreamSocketsMessaging::GetChannelForConnection(
    uint64_t connection_id) {
  CHECK_LT(connection_id, active_channels_.size());
  return active_channels_[connection_id];
}

void StreamSocketsMessaging::AwaitNextMessage() {
  // If we have no active channels, we cannot receive any messages, so we return
  // immediately.
  if (active_channels_.size() == 0)
    return;
  // Otherwise, let's make sure we have an outstanding async receive request for
  // each fo them.
  for (vector<boost::shared_ptr<StreamSocketsChannel<BaseMessage> > >::
       const_iterator chan_iter = active_channels_.begin();
       chan_iter != active_channels_.end();
       ++chan_iter) {
    boost::shared_ptr<StreamSocketsChannel<BaseMessage> > chan =
        *chan_iter;
    if (!channel_recv_envelopes_.count(chan)) {
      // No outstanding receive request for this channel, so create one
      Envelope<BaseMessage>* envelope = new Envelope<BaseMessage>();
      channel_recv_envelopes_.insert(
          pair<boost::shared_ptr<StreamSocketsChannel<BaseMessage> >,
          Envelope<BaseMessage>*>(chan, envelope));
      VLOG(2) << "MA replenishing envelope for channel " << chan
              << " at " << envelope;
      chan->RecvA(envelope,
                  boost::bind(&StreamSocketsMessaging::HandleAsyncMessageRecv,
                              this, boost::asio::placeholders::error,
                              boost::asio::placeholders::bytes_transferred,
                              chan));
    }
  }
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
}

void StreamSocketsMessaging::HandleAsyncMessageRecv(
    const boost::system::error_code& error,
    size_t bytes_transferred,
    shared_ptr<StreamSocketsChannel<BaseMessage> > chan) {
  if (error) {
    LOG(WARNING) << "Error receiving in MA";
    // TODO(malte): think about clearing up state here. Should we consider the
    // envelope as having been consumed?
    // XXX(malte): Do we need to unlock/signal here?
    return;
  }
  CHECK(channel_recv_envelopes_.count(chan));
  Envelope<BaseMessage>* envelope = channel_recv_envelopes_[chan];
  VLOG(2) << "Received in MA: " << *envelope << " ("
          << bytes_transferred << ")";
  channel_recv_envelopes_.erase(chan);
  delete envelope;
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

void StreamSocketsMessaging::Listen(const string& endpoint_uri) {
  // Parse endpoint URI into hostname and port
  string hostname = URITools::GetHostnameFromURI(endpoint_uri);
  string port = URITools::GetPortFromURI(endpoint_uri);

  VLOG(1) << "Creating an async TCP server on port " << port
          << " on endpoint " << hostname << "(" << endpoint_uri << ")";
  tcp_server_ = new AsyncTCPServer(hostname, port, shared_from_this());
  boost::thread t(boost::bind(&AsyncTCPServer::Run, tcp_server_));
  VLOG(1) << "AsyncTCPServer's main thread running as " << t.get_id();
}

bool StreamSocketsMessaging::ListenReady() {
  if (tcp_server_ != NULL)
    return tcp_server_->listening();
  else
    return false;
}

/*void StreamSocketsMessaging::SendOnConnection(uint64_t connection_id) {
  VLOG(2) << "Messaging adapter sending on connection " << connection_id;
  // TODO(malte): Hack -- we spin until the connection is ready. This is
  // required to avoid race conditions where a messaging adapter is trying to
  // send on a connection before it is ready. This can occur due to the
  // asynchronous, multi-threaded nature of the TCP server.
  while (!tcp_server_->connection(connection_id)->Ready()) {
    VLOG_EVERY_N(2, 1000) << "Waiting for connection " << connection_id
                          << " to be ready to send...";
    boost::this_thread::yield();
  }
  // Actually send the data on the (now ready) TCP connection
  //tcp_server_->connection(connection_id)->Send();
  LOG(FATAL) << "Unimplemented!";
}*/

void StreamSocketsMessaging::StopListen() {
  if (tcp_server_ != NULL) {
    VLOG(2) << "Stopping async TCP server at " << tcp_server_
            << "...";
    tcp_server_->Stop();
  }
  // t.join()
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament
