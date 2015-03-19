// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H

#include "platforms/unix/stream_sockets_channel.h"

#include <vector>
#include <string>

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

using boost::asio::ip::tcp;
using boost::asio::io_service;

// ----------------------------
// StreamSocketsChannel
// ----------------------------

template <class T>
StreamSocketsChannel<T>::StreamSocketsChannel(StreamSocketType type)
  : client_io_service_(new io_service),
    client_socket_(NULL),
    channel_ready_(false),
    type_(type) {
  switch (type) {
    case SS_TCP: {
      // Set up two TCP endpoints (?)
      // TODO(malte): investigate if we can use a scoped pointer here
      VLOG(2) << "Setup for TCP endpoints";
      break; }
    case SS_UNIX:
      LOG(FATAL) << "Unimplemented!";
      break;
    default:
      LOG(FATAL) << "Unknown stream socket type!";
  }
}

template <class T>
StreamSocketsChannel<T>::StreamSocketsChannel(
    TCPConnection::connection_ptr connection)
  : //client_io_service_(&(connection->socket()->get_io_service())),
    client_socket_(connection->socket()),
    client_connection_(connection),
    channel_ready_(false),
    type_(SS_TCP) {
  VLOG(2) << "Creating new channel around socket at " << client_socket_;
  if (client_socket_->is_open()) {
    channel_ready_ = true;
  }
}

template <class T>
StreamSocketsChannel<T>::~StreamSocketsChannel() {
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

template <class T>
void StreamSocketsChannel<T>::Close() {
  // end the connection
  VLOG(2) << "Shutting down channel " << *this << "'s socket...";
  channel_ready_ = false;
  if (!client_connection_) {
    // The channel has ownership of the client socket
    if (client_socket_) {
      boost::system::error_code ec;
      client_socket_->shutdown(tcp::socket::shutdown_both, ec);
      if (ec)
        LOG(WARNING) << "Error shutting down connections on socket for "
                     << "connection at " << this << ": " << ec.message();
    }
  } else {
    // The channel was constructed around an existing connection, so it does not
    // have ownership of the socket. Ask the connection to terminate instead.
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

template <class T>
bool StreamSocketsChannel<T>::Establish(const string& endpoint_uri) {
  // If this channel already has an active socket, issue a warning and close it
  // down before establishing a new one.
  if (!client_connection_ && client_socket_ && client_socket_->is_open()) {
    LOG(WARNING) << "Establishing a new connection on channel " << this
                 << ", despite already having one established. The previous "
                 << "connection will be terminated.";
    client_connection_->Close();
    channel_ready_ = false;
  }
  CHECK(!(client_connection_ && client_io_service_));

  // Parse endpoint URI into hostname and port
  string hostname = URITools::GetHostnameFromURI(endpoint_uri);
  string port = URITools::GetPortFromURI(endpoint_uri);

  // Now make the connection
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
    return true;
  }
}

template <typename T>
const string StreamSocketsChannel<T>::LocalEndpointString() {
  if (client_connection_)
    return client_connection_->LocalEndpointString();
  string address;
  string port;
  string protocol;
  string endpoint;
  boost::system::error_code ec;
  switch (type_) {
    case SS_TCP:
      protocol = "tcp:";
      address = client_socket_->local_endpoint(ec).address().to_string();
      port = to_string<uint64_t>(client_socket_->local_endpoint(ec).port());
      if (ec)
        return "";
      else
        return protocol + address + ":" + port;
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

// Ready check
template <class T>
bool StreamSocketsChannel<T>::Ready() {
  return (channel_ready_ && client_socket_->is_open());
}

// Get textual description of remote endpoint
template <typename T>
const string StreamSocketsChannel<T>::RemoteEndpointString() {
  if (client_connection_)
    return client_connection_->RemoteEndpointString();
  boost::system::error_code ec;
  tcp::endpoint ept = client_socket_->remote_endpoint(ec);
  if (ec)
    return "";
  else
    return EndpointToString(ept);
}

// Synchronous send
template <class T>
bool StreamSocketsChannel<T>::SendS(const Envelope<T>& message) {
  VLOG(2) << "Trying to send message of size " << message.size()
          << " on channel " << *this;
  uint64_t msg_size = message.size();
  vector<char> buf(msg_size);
  CHECK(message.Serialize(&buf[0], message.size()));
  // Send data size
  boost::system::error_code error;
  uint64_t len;
  uint64_t msg_size_endian = htobe64(msg_size);

  len = boost::asio::write(
      *client_socket_, boost::asio::buffer(
          reinterpret_cast<char*>(&msg_size_endian), sizeof(msg_size_endian)),
             boost::asio::transfer_exactly(sizeof(uint64_t)), error);
  if (error || len != sizeof(uint64_t)) {
    LOG(ERROR) << "Error sending size preamble on connection: "
               << error.message();
    return false;
  }
  // Send the data
  len = boost::asio::write(
      *client_socket_, boost::asio::buffer(buf, msg_size),
             boost::asio::transfer_exactly(msg_size), error);
  if (error || len != msg_size) {
    LOG(ERROR) << "Error sending message on connection: "
               << error.message();
    return false;
  } else {
    VLOG(2) << "Sent " << len << " bytes of protobuf data: "
            << message;
  }
  return true;
}

// Asynchronous send
// N.B.: error handling is deferred to the callback handler, which takes a
// boost::system:error_code.
template <class T>
bool StreamSocketsChannel<T>::SendA(
    const Envelope<T>& message,
    typename AsyncSendHandler<T>::type callback) {
  LOG(FATAL) << "Should not currently use broken SendA!";
  VLOG(2) << "Trying to asynchronously send message: " << message;
  uint64_t msg_size = message.size();
  // XXX(malte): not freed correctly!
  vector<char>* buf = new vector<char>(msg_size);
  CHECK(message.Serialize(&buf[0], msg_size));

  uint64_t msg_size_endian = htobe64(msg_size);
  // Synchronously send data size first
  boost::asio::async_write(
      *client_socket_, boost::asio::buffer(
          reinterpret_cast<char*>(&msg_size_endian), sizeof(msg_size_endian)),
      callback);
  // Send the data
  boost::asio::async_write(
      *client_socket_, boost::asio::buffer(buf, msg_size), callback);
  return true;
}

// Synchronous recieve -- blocks until the next message is received.
template <class T>
bool StreamSocketsChannel<T>::RecvS(Envelope<T>* message) {
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
    return false;
  } else {
    VLOG(2) << "Read " << len << " bytes of protobuf data...";
  }
  CHECK_GT(len, 0);
  CHECK_EQ(len, msg_size);
  // XXX(malte): data copy here -- optimize away if possible.
  return (message->Parse(&buf[0], len));
}

// Asynchronous receive -- does not block.
template <class T>
bool StreamSocketsChannel<T>::RecvA(
    Envelope<T>* message,
    typename AsyncRecvHandler<T>::type callback) {
  VLOG(2) << "In RecvA, waiting for next message";
  if (!Ready()) {
    LOG(WARNING) << "Tried to read from channel " << this
                 << ", which is not ready; read failed.";
    return false;
  }
  // Obtain the lock on the async receive buffer.
  async_recv_lock_.lock();
  async_recv_buffer_vec_.reset(new vector<char>(sizeof(uint64_t)));
  async_recv_buffer_.reset(new boost::asio::mutable_buffers_1(
      reinterpret_cast<char*>(&(*async_recv_buffer_vec_)[0]),
                                sizeof(uint64_t)));
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

// Second stage of asynchronous receive, which calls async_recv again in order
// to get the actual message data.
// Called with the async_recv_lock_ mutex held. It is either released before
// returning (error path) or maintained for RecvAThirdStage to release.
template <class T>
void StreamSocketsChannel<T>::RecvASecondStage(
    const boost::system::error_code& error, const size_t bytes_read,
    Envelope<T>* final_envelope,
    typename AsyncRecvHandler<T>::type final_callback) {
  if (error || bytes_read != sizeof(uint64_t)) {
    LOG(ERROR) << "Error reading from connection: " << error.message()
               << "; read " << bytes_read << " bytes, expected "
               << sizeof(uint64_t);
    final_callback(error, bytes_read, final_envelope);
    async_recv_lock_.unlock();
    return;
  }
  // ... we can get away with a simple CHECK here and assume that we have some
  // incoming data available.
  CHECK_EQ(sizeof(uint64_t), bytes_read);
  // Nasty cast to get message size indicator received (after endian conversion)
  uint64_t msg_size =
    be64toh(*reinterpret_cast<uint64_t*>(&(*async_recv_buffer_vec_)[0]));
  CHECK_GT(msg_size, 0);
  // XXX(malte): This is a nasty hack to highlight bugs in the channel logic.
  CHECK_LT(msg_size, 35000) << "Received implausibly large message "
                            << "from " << RemoteEndpointString();
  VLOG(2) << "RecvA: size of incoming protobuf from" << RemoteEndpointString()
          << "is " << msg_size << " bytes.";
  // We still hold the async_recv_lock_ mutex here.
  async_recv_buffer_vec_.reset(new vector<char>(msg_size));
  async_recv_buffer_.reset(new boost::asio::mutable_buffers_1(
      reinterpret_cast<char*>(&(*async_recv_buffer_vec_)[0]), msg_size));
  async_read(*client_socket_, *async_recv_buffer_,
             boost::asio::transfer_exactly(msg_size),
             boost::bind(&StreamSocketsChannel<T>::RecvAThirdStage,
                         this,
                         boost::asio::placeholders::error,
                         boost::asio::placeholders::bytes_transferred,
                         msg_size, final_envelope, final_callback));
  // Second stage done.
}

// Third stage of asynchronous receive, which finalizes the message reception by
// parsing the received data.
// Called with the async_recv_lock_ mutex held, but releases it before
// returning.
template <class T>
void StreamSocketsChannel<T>::RecvAThirdStage(
    const boost::system::error_code& error,
    const size_t bytes_read, uint64_t message_size,
    Envelope<T>* final_envelope,
    typename AsyncRecvHandler<T>::type final_callback) {
  VLOG(2) << "Read " << bytes_read << " bytes.";
  if (error == boost::asio::error::eof) {
    VLOG(1) << "Received EOF, connection terminating!";
    final_callback(error, bytes_read, final_envelope);
    async_recv_lock_.unlock();
    return;
  } else if (error) {
    LOG(ERROR) << "Error reading from connection: "
               << error.message() << "; read " << bytes_read
               << " bytes, expected " << message_size;
    final_callback(error, bytes_read, final_envelope);
    async_recv_lock_.unlock();
    return;
  } else {
    VLOG(2) << "Read " << bytes_read << " bytes of protobuf data...";
  }
  CHECK_GT(bytes_read, 0);
  CHECK_EQ(bytes_read, message_size);
  VLOG(2) << "About to parse message";
  if (!final_envelope->Parse(&(*async_recv_buffer_vec_)[0], bytes_read)) {
    LOG(ERROR) << "Failed to parse protobuf message of " << bytes_read
               << " bytes!";
  }
  // Drop the lock
  VLOG(2) << "Unlocking async receive buffer";
  // Invoke the original callback
  // XXX(malte): potential race condition -- someone else may finish and invoke
  // the callback before we do (although this is very unlikely).
  VLOG(2) << "About to invoke final async recv callback!";
  final_callback(error, bytes_read, final_envelope);
  async_recv_lock_.unlock();
}

template <class T>
ostream& StreamSocketsChannel<T>::ToString(ostream* stream) const {
  return *stream << "(StreamSocket,type=" << type_ << ",at=" << this << ")";
}


}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
