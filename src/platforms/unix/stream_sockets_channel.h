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

// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H

#include <boost/asio.hpp>

#include <string>
#include <vector>

#include <boost/noncopyable.hpp>

#include "base/common.h"
#include "misc/envelope.h"
#include "misc/protobuf_envelope.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Channel.
template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T>,
  public boost::enable_shared_from_this<StreamSocketsChannel<T> >,
  private boost::noncopyable {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;
  typedef StreamSocketsChannel<T> type;
  typedef shared_ptr<type> ptr_type;

  explicit StreamSocketsChannel(StreamSocketType type)
    : async_recv_buffer_(NULL),
      async_recv_buffer_vec_(NULL),
      client_io_service_(new io_service),
      client_socket_(NULL),
      channel_ready_(false),
      type_(type) {
    switch (type) {
    case SS_TCP:
      VLOG(2) << "Setup for TCP endpoints";
      break;
    case SS_UNIX:
      LOG(FATAL) << "Unimplemented!";
      break;
    default:
      LOG(FATAL) << "Unknown stream socket type!";
    }
  }

  explicit StreamSocketsChannel(TCPConnection::connection_ptr connection)
    : async_recv_buffer_(NULL),
      async_recv_buffer_vec_(NULL),
      client_socket_(connection->socket()),
      client_connection_(connection),
      channel_ready_(false),
      type_(SS_TCP) {
    VLOG(2) << "Creating new channel around socket at " << client_socket_;
    if (client_socket_->is_open()) {
      channel_ready_ = true;
    }
  }

  virtual ~StreamSocketsChannel() {
    VLOG(2) << "Channel destructor called!";
    // The user may already have manually cleaned up. If not, we do so now.
    if (Ready()) {
      channel_ready_ = false;
      if (io_service_work_)
        io_service_work_.reset(); // ~work();
      Close();
    }
    VLOG(2) << "Channel at " << this << " destroyed.";
  }

  void Close() {
    // end the connection
    VLOG(2) << "Shutting down channel " << *this << "'s socket...";
    channel_ready_ = false;
    if (!client_connection_) {
      // The channel has ownership of the client socket
      if (client_socket_) {
        boost::system::error_code ec;
        client_socket_->shutdown(tcp::socket::shutdown_both, ec);
        if (ec) {
          LOG(WARNING) << "Error shutting down connections on socket for "
                       << "connection at " << this << ": " << ec.message();
        }
      }
    } else {
      // The channel was constructed around an existing connection, so it does
      // not have ownership of the socket. Ask the connection to terminate
      // instead.
      client_connection_->Close();
      client_connection_.reset();
      client_socket_ = NULL;
    }
    CHECK(!(client_connection_ && client_io_service_));
    if (client_io_service_) {
      VLOG(2) << "Stopping client IO service from thread "
              << boost::this_thread::get_id();
      client_io_service_->stop();
    }
  }

  bool Establish(const string& endpoint_uri) {
    // If this channel already has an active socket, issue a warning and close
    // it down before establishing a new one.
    if (!client_connection_ && client_socket_ && client_socket_->is_open()) {
      LOG(WARNING) << "Establishing a new connection on channel " << this
                   << ", despite already having one established. The previous "
                   << "connection will be terminated.";
      client_socket_->close();
      channel_ready_ = false;
    }
    CHECK(!(client_connection_ && client_io_service_));

    // Parse endpoint URI into hostname and port
    string hostname = URITools::GetHostnameFromURI(endpoint_uri);
    string port = URITools::GetPortFromURI(endpoint_uri);

    // Now make the connection
    CHECK_NE(endpoint_uri, "");
    VLOG(1) << "Establishing a new channel (TCP connection), "
            << "remote endpoint is " << endpoint_uri;
    tcp::resolver resolver(*client_io_service_);
    tcp::resolver::query query(hostname, port);
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;

    if (client_socket_)
      LOG(WARNING) << "Replacing an existing client socket!";
    client_socket_ = new tcp::socket(*client_io_service_);
    boost::system::error_code error = boost::asio::error::host_not_found;
    while (error && endpoint_iterator != end) {
      if (client_socket_->is_open())
        client_socket_->close();
      client_socket_->connect(*endpoint_iterator++, error);
    }
    if (error) {
      LOG(ERROR) << "Failed to establish a stream socket channel to remote "
                 << "endpoint " << endpoint_uri << ". Error: "
                 << error.message();
      return false;
    } else {
      io_service_work_.reset(new io_service::work(*client_io_service_));
      VLOG(2) << "Client: we appear to have connected successfully; "
              << "local endpoint: " << LocalEndpointString();
      shared_ptr<boost::thread> thread(new boost::thread(
          boost::bind(&boost::asio::io_service::run, client_io_service_)));
      VLOG(2) << "Created IO service thread " << thread->get_id();
      channel_ready_ = true;
      cached_remote_endpoint_ = endpoint_uri;
      return true;
    }
  }

  const string LocalEndpointString() {
    if (client_connection_)
      return client_connection_->LocalEndpointString();
    string address;
    string port;
    string protocol;
    boost::system::error_code ec;
    switch (type_) {
    case SS_TCP:
      protocol = "tcp:";
      address = client_socket_->local_endpoint(ec).address().to_string();
      port = to_string<uint64_t>(client_socket_->local_endpoint(ec).port());
      if (ec) {
        return "";
      } else {
        return protocol + address + ":" + port;
      }
    case SS_UNIX:
      // XXX(malte): This does not actually work (I think), because ASIO's UNIX
      // socket representation does not have an address() member. We need to
      // figure out how to deal with this when implementing something other than
      // TCP. path() in local::stream_protocol::endpoint seems like a good
      // choice.
      protocol = "unix:";
      //address = client_socket_->remote_endpoint().path();
      return protocol + address;
    default:
      LOG(FATAL) << "Unknown stream socket type " << type_;
      return "";
    }
  }

  bool Ready() {
    return (channel_ready_ && client_socket_->is_open());
  }

  bool RecvA(misc::Envelope<T>* message,
             typename AsyncRecvHandler<T>::type callback) {
    VLOG(2) << "In RecvA, waiting for next message";
    if (!Ready()) {
      LOG(WARNING) << "Tried to read from channel " << this
                   << ", which is not ready; read failed.";
      return false;
    }
    // Obtain the lock on the async receive buffer.
    async_recv_lock_.lock();
    CHECK_EQ(async_recv_buffer_vec_, static_cast<char*>(NULL));
    CHECK_EQ(async_recv_buffer_,
             static_cast<boost::asio::mutable_buffers_1*>(NULL));
    async_recv_buffer_vec_ = reinterpret_cast<char*>(malloc(sizeof(uint64_t)));
    async_recv_buffer_ =
      new boost::asio::mutable_buffers_1(async_recv_buffer_vec_,
                                         sizeof(uint64_t));
    // Asynchronously read the incoming protobuf message length and invoke the
    // second stage of the receive call once we have it.
    async_read(*client_socket_, *async_recv_buffer_,
               boost::asio::transfer_exactly(sizeof(uint64_t)),
               boost::bind(&StreamSocketsChannel<T>::RecvASecondStage,
                           this,
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred,
                           message, callback));
    // First stage of RecvA always succeeds.
    return true;
  }

  /**
   * Synchronous receive -- blocks until the next message is received.
   */
  bool RecvS(misc::Envelope<T>* message) {
    boost::lock_guard<boost::mutex> lock(sync_send_lock_);
    VLOG(2) << "In RecvS, polling for next message";
    if (!Ready()) {
      LOG(WARNING) << "Tried to read from channel " << this
                   << ", which is not ready; read failed.";
      return false;
    }
    uint64_t len;
    vector<char> size_buf(sizeof(uint64_t));
    boost::asio::mutable_buffers_1 size_m_buf(
        reinterpret_cast<char*>(&size_buf[0]), sizeof(uint64_t));
    boost::system::error_code error;
    // Read the incoming protobuf message length
    // N.B.: read() blocks until the buffer has been filled, i.e. an entire
    // uint64_t has been read.
    len = read(*client_socket_, size_m_buf,
               boost::asio::transfer_exactly(sizeof(uint64_t)), error);
    if (error || len != sizeof(uint64_t)) {
      LOG(ERROR) << "Error reading from connection on channel " << *this
                 << "(len: " << len << ", expected: " << sizeof(uint64_t) << ")"
                 << ": " << error.message();
      if (error)
        HandleIOError(error);
      return false;
    }
    // ... we can get away with a simple CHECK here and assume that we have some
    // incoming data available.
    CHECK_EQ(sizeof(uint64_t), len);
    uint64_t msg_size = be64toh(*reinterpret_cast<uint64_t*>(&size_buf[0]));
    CHECK_GT(msg_size, 0);
    VLOG(3) << "RecvS: size of incoming protobuf from" << RemoteEndpointString()
            << "is " << msg_size << " bytes.";
    // XXX(malte): This is a nasty hack to highlight bugs in the channel logic.
    CHECK_LT(msg_size, 35000) << "Received implausibly large message size "
                              << "from " << RemoteEndpointString();
    vector<char> buf(msg_size);
    len = read(*client_socket_,
               boost::asio::mutable_buffers_1(&buf[0], msg_size),
               boost::asio::transfer_exactly(msg_size), error);
    VLOG(2) << "Read " << len << " bytes.";

    if (error == boost::asio::error::eof) {
      VLOG(1) << "Received EOF, connection terminating!";
      return false;
    } else if (error) {
      LOG(ERROR) << "Error reading from connection: "
                 << error.message();
      HandleIOError(error);
      return false;
    } else {
      VLOG(2) << "Read " << len << " bytes of protobuf data...";
    }
    CHECK_GT(len, 0);
    CHECK_EQ(len, msg_size);
    // XXX(malte): data copy here -- optimize away if possible.
    return (message->Parse(&buf[0], len));
  }

  /**
   * Get textual description of remote endpoint.
   */
  const string RemoteEndpointString() {
    if (client_connection_)
      return client_connection_->RemoteEndpointString();
    boost::system::error_code ec;
    tcp::endpoint ept = client_socket_->remote_endpoint(ec);
    if (ec)
      return cached_remote_endpoint_;
    else
      return EndpointToString(ept);
  }

  bool SendS(const misc::Envelope<T>& message) {
    boost::lock_guard<boost::mutex> lock(sync_send_lock_);
    VLOG(2) << "Trying to send message of size " << message.size()
            << " on channel " << *this;
    uint64_t msg_size = message.size();
    vector<char> buf(msg_size);
    CHECK(message.Serialize(&buf[0], message.size()));
    // Send data size
    boost::system::error_code error;
    uint64_t msg_size_endian = htobe64(msg_size);

    uint64_t len = boost::asio::write(
        *client_socket_, boost::asio::buffer(
            reinterpret_cast<char*>(&msg_size_endian), sizeof(msg_size_endian)),
        boost::asio::transfer_exactly(sizeof(uint64_t)), error);
    if (error || len != sizeof(uint64_t)) {
      LOG(ERROR) << "Error sending size preamble on connection: "
                 << error.message();
      if (error)
        HandleIOError(error);
      return false;
    }
    // Send the data
    len = boost::asio::write(
        *client_socket_, boost::asio::buffer(buf, msg_size),
        boost::asio::transfer_exactly(msg_size), error);
    if (error || len != msg_size) {
      LOG(ERROR) << "Error sending message on connection: "
                 << error.message();
      if (error)
        HandleIOError(error);
      return false;
    } else {
      VLOG(2) << "Sent " << len << " bytes of protobuf data: "
              << message;
    }
    return true;
  }

  /**
   * Asynchronous send.
   * N.B.: error handling is deferred to the callback handler, which takes a
   * boost::system:error_code.
   */
  bool SendA(const misc::Envelope<T>& message,
             typename AsyncSendHandler<T>::type callback) {
    LOG(FATAL) << "Should not currently use broken SendA!";
    VLOG(2) << "Trying to asynchronously send message: " << message;
    uint64_t msg_size = message.size();
    // XXX(malte): not freed correctly!
    vector<char>* buf = new vector<char>(msg_size);
    CHECK(message.Serialize(&buf[0], msg_size));

    uint64_t msg_size_endian = htobe64(msg_size);
    // Synchronously send data size first
    // XXX(malte): broken, sends async
    boost::asio::async_write(
        *client_socket_, boost::asio::buffer(
             reinterpret_cast<char*>(&msg_size_endian),
             sizeof(msg_size_endian)),
        callback);
    // Send the data
    boost::asio::async_write(
        *client_socket_, boost::asio::buffer(buf, msg_size), callback);
    return true;
  }

  ostream& ToString(ostream* stream) const {
    return *stream << "(StreamSocket,type=" << type_ << ",at=" << this << ")";
  }

 protected:
  void HandleIOError(boost::system::error_code error) {
    if (error == boost::asio::error::broken_pipe) {
      // Connection has gone away; attempt to reconnect
      LOG(ERROR) << "Trying to reconnect broken underlying connection "
                 << "for channel " << *this << "(" << LocalEndpointString()
                 << " -> " << RemoteEndpointString() << ")";
      if (client_socket_)
        Establish(RemoteEndpointString());
      else if (client_connection_)
        LOG(ERROR) << "Cannot recover server-side connection on channel; "
                   << "client must reconnect";
    } else if (error == boost::asio::error::eof) {
      // EOF
      VLOG(2) << "Connection to " << RemoteEndpointString() << " was closed "
              << "by remote end.";
    } else {
      // We don't currently handle anything else
      LOG(ERROR) << "Don't currently know how to handle error of type "
                 << error.message();
    }
  }

  /**
   * Second stage of asynchronous receive, which calls async_recv again in order
   * to get the actual message data.
   * Called with the async_recv_lock_ mutex held. It is either released before
   * returning (error path) or maintained for RecvAThirdStage to release.
   */
  void RecvASecondStage(const boost::system::error_code& error,
                        const size_t bytes_read,
                        Envelope<T>* final_envelope,
                        typename AsyncRecvHandler<T>::type final_callback) {
    if (error || bytes_read != sizeof(uint64_t)) {
      if (error != boost::asio::error::eof) {
        LOG(ERROR) << "Error reading from connection: " << error.message()
                   << "; read " << bytes_read << " bytes, expected "
                   << sizeof(uint64_t);
      }
      if (error)
        HandleIOError(error);
      async_recv_lock_.unlock();
      final_callback(error, bytes_read, final_envelope);
      return;
    }
    // ... we can get away with a simple CHECK here and assume that we have some
    // incoming data available.
    CHECK_EQ(sizeof(uint64_t), bytes_read);
    // Nasty cast to get message size indicator received (after endian
    // conversion)
    uint64_t msg_size =
      be64toh(*reinterpret_cast<uint64_t*>(async_recv_buffer_vec_));
    CHECK_GT(msg_size, 0) << "Received message of length 0 from "
                          << RemoteEndpointString();
    // XXX(malte): This is a nasty hack to highlight bugs in the channel logic.
    CHECK_LT(msg_size, 1024*1024) << "Received implausibly large message "
                                  << "from " << RemoteEndpointString();
    VLOG(2) << "RecvA: size of incoming protobuf from" << RemoteEndpointString()
            << "is " << msg_size << " bytes.";
    // We still hold the async_recv_lock_ mutex here.
    free(async_recv_buffer_vec_);
    async_recv_buffer_vec_ = reinterpret_cast<char*>(malloc(msg_size));
    VLOG(2) << "New async recv buffer is at "
            << static_cast<void*>(async_recv_buffer_vec_);
    delete async_recv_buffer_;
    async_recv_buffer_ =
      new boost::asio::mutable_buffers_1(async_recv_buffer_vec_, msg_size);
    async_read(*client_socket_, *async_recv_buffer_,
               boost::asio::transfer_exactly(msg_size),
               boost::bind(&StreamSocketsChannel<T>::RecvAThirdStage,
                           this,
                           boost::asio::placeholders::error,
                           boost::asio::placeholders::bytes_transferred,
                           msg_size, final_envelope, final_callback));
  }

  /**
   * Third stage of asynchronous receive, which finalizes the message reception
   * by parsing the received data.
   * Called with the async_recv_lock_ mutex held, but releases it before
   * returning.
   */
  void RecvAThirdStage(const boost::system::error_code& error,
                       const size_t bytes_read, uint64_t message_size,
                       Envelope<T>* final_envelope,
                       typename AsyncRecvHandler<T>::type final_callback) {
    VLOG(2) << "Read " << bytes_read << " bytes.";
    if (error == boost::asio::error::eof) {
      VLOG(1) << "Received EOF, connection terminating!";
      async_recv_lock_.unlock();
      final_callback(error, bytes_read, final_envelope);
      return;
    } else if (error) {
      LOG(ERROR) << "Error reading from connection: "
                 << error.message() << "; read " << bytes_read
                 << " bytes, expected " << message_size;
      if (error)
        HandleIOError(error);
      async_recv_lock_.unlock();
      final_callback(error, bytes_read, final_envelope);
      return;
    } else {
      VLOG(2) << "Read " << bytes_read << " bytes of protobuf data...";
    }
    CHECK_GT(bytes_read, 0);
    CHECK_EQ(bytes_read, message_size);
    VLOG(2) << "About to parse message";
    if (!final_envelope->Parse(async_recv_buffer_vec_, bytes_read)) {
      LOG(ERROR) << "Failed to parse protobuf message of " << bytes_read
                 << " bytes!";
    }
    free(async_recv_buffer_vec_);
    delete async_recv_buffer_;
    async_recv_buffer_vec_ = NULL;
    async_recv_buffer_ = NULL;
    // Invoke the original callback
    // XXX(malte): potential race condition -- someone else may finish and
    // invoke the callback before we do (although this is very unlikely).
    VLOG(2) << "About to invoke final async recv callback!";
    final_callback(error, bytes_read, final_envelope);
    // Drop the lock
    VLOG(2) << "Unlocking async receive buffer";
    async_recv_lock_.unlock();
  }

 private:
  boost::mutex sync_recv_lock_;
  boost::mutex sync_send_lock_;
  // Async receive buffer data structures and lock
  boost::mutex async_recv_lock_;
  //scoped_ptr<boost::asio::mutable_buffers_1> async_recv_buffer_;
  boost::asio::mutable_buffers_1* async_recv_buffer_;
  //scoped_ptr<vector<char> > async_recv_buffer_vec_;
  char* async_recv_buffer_vec_;
  // TCP and io_service data structures
  shared_ptr<boost::asio::io_service> client_io_service_;
  scoped_ptr<boost::asio::io_service::work> io_service_work_;
  // This cannot be a shared_ptr, since the client socket may disappear under
  // the channel's feet at any point in time.
  boost::asio::ip::tcp::socket* client_socket_;
  TCPConnection::connection_ptr client_connection_;
  // A cached copy of the channel's remote endpoint URI. When the underlying
  // socket/connection have been closed, this cannot be dynamically derived, so
  // we need a copy here in order to re-establish connectivity.
  string cached_remote_endpoint_;
  bool channel_ready_;
  StreamSocketType type_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
