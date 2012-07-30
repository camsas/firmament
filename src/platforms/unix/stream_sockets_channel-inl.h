// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H

#include "platforms/unix/stream_sockets_channel.h"

#include <boost/asio.hpp>

#include <string>

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

using boost::asio::ip::tcp;
using boost::asio::io_service;
using boost::asio::socket_base;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// ----------------------------
// StreamSocketsChannel
// ----------------------------

template <class T>
StreamSocketsChannel<T>::StreamSocketsChannel(StreamSocketType type)
  : client_io_service_(new io_service),
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
StreamSocketsChannel<T>::StreamSocketsChannel(tcp::socket* socket)
  : client_io_service_(&(socket->get_io_service())),
    client_socket_(socket),
    channel_ready_(false),
    type_(SS_TCP) {
  if (client_socket_->is_open()) {
    channel_ready_ = true;
  }
}

template <class T>
StreamSocketsChannel<T>::~StreamSocketsChannel() {
  // The user may already have manually cleaned up. If not, we do so now.
  if (Ready()) {
    channel_ready_ = false;
    Close();
  }
}

template <class T>
void StreamSocketsChannel<T>::Establish(const string& endpoint_uri) {
  // If this channel already has an active socket, issue a warning and close it
  // down before establishing a new one.
  if (client_socket_.get() != NULL && client_socket_->is_open()) {
    LOG(WARNING) << "Establishing a new connection on channel " << this
                 << ", despite already having one established. The previous "
                 << "connection will be terminated.";
    client_socket_->shutdown(socket_base::shutdown_both);
    channel_ready_ = false;
  }

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

  client_socket_.reset(new tcp::socket(*client_io_service_));
  boost::system::error_code error = boost::asio::error::host_not_found;
  while (error && endpoint_iterator != end) {
    client_socket_->close();
    client_socket_->connect(*endpoint_iterator++, error);
  }
  if (error) {
    throw boost::system::system_error(error);
  } else {
    VLOG(2) << "Client: we appear to have connected successfully...";
    channel_ready_ = true;
  }
}

// Ready check
template <class T>
bool StreamSocketsChannel<T>::Ready() {
  return (channel_ready_ && client_socket_->is_open());
}

// Synchronous send
template <class T>
bool StreamSocketsChannel<T>::SendS(const T& message) {
  VLOG(1) << "Trying to send message of size " << message.ByteSize()
          << " on channel " << *this;
  size_t msg_size = message.ByteSize();
  vector<char> buf(msg_size);
  CHECK(message.SerializeToArray(&buf[0], message.ByteSize()));
  // Send data size
  boost::asio::write(
      *client_socket_, boost::asio::buffer(
          reinterpret_cast<char*>(&msg_size), sizeof(msg_size)));
          /*boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred));*/
  // Send the data
  boost::asio::write(
      *client_socket_, boost::asio::buffer(buf, msg_size));
      /*boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));*/
  return true;
}

// Asynchronous send
template <class T>
bool StreamSocketsChannel<T>::SendA(const T& message) {
  VLOG(1) << "Trying to send message: " << &message;
  LOG(FATAL) << "Unimplemented!";
  return false;
}

// Synchronous recieve -- blocks until the next message is received.
template <class T>
bool StreamSocketsChannel<T>::RecvS(T* message) {
  if (!Ready()) {
    LOG(WARNING) << "Tried to read from channel " << this << ", which is not ready; read failed.";
    return false;
  }
  VLOG(2) << "In RecvS, polling for next message";
  size_t len;
  vector<char> size_buf(sizeof(size_t));
  boost::asio::mutable_buffers_1 size_m_buf(reinterpret_cast<char*>(&size_buf[0]), sizeof(size_t));
  boost::system::error_code error;
  // Read the incoming protobuf message length
  // N.B.: read() blocks until the buffer has been filled, i.e. an entire size_t
  // has been read.
  len = read(*client_socket_, size_m_buf);
  // ... we can get away with a simple CHECK here and assume that we have some
  // incoming data available.
  CHECK_EQ(sizeof(size_t), len);
  size_t msg_size = *reinterpret_cast<size_t*>(&size_buf[0]);
  CHECK_GT(msg_size, 0);
  VLOG(2) << "Size of incoming protobuf is " << msg_size << " bytes.";
  vector<char> buf(msg_size);
  size_t total = 0;
  do {
    VLOG(3) << "calling read_some";
    //CHECK_LE(total, size_buf);
    len = client_socket_->read_some(boost::asio::mutable_buffers_1(&buf[0], msg_size), error);

    if (error == boost::asio::error::eof) {
      VLOG(2) << "Received EOF, connection terminating!";
      break;
    } else if (error) {
      throw boost::system::system_error(error);
    } else {
      VLOG(2) << "Read " << len << " bytes of protobuf data...";
      total += len;
    }
  } while (client_socket_->available() > 0);
  VLOG(2) << "Read a total of " << total << " protobuf data bytes.";
  CHECK_GT(total, 0);
  CHECK_EQ(total, msg_size);
  return (message->ParseFromArray(&buf[0], total));
}

// Asynchronous receive -- does not block.
template <class T>
bool StreamSocketsChannel<T>::RecvA(T* message) {
  VLOG(1) << "Receiving into " << message;
  LOG(FATAL) << "Unimplemented!";
  return false;
}

template <class T>
void StreamSocketsChannel<T>::Close() {
  // end the connection
  VLOG(2) << "Shutting down channel " << *this << "'s socket...";
  client_socket_->shutdown(socket_base::shutdown_both);
  channel_ready_ = false;
}

template <class T>
ostream& StreamSocketsChannel<T>::ToString(ostream& stream) const {
  return stream << "(StreamSocket,type=" << type_ << ",at=" << this << ")";
}


}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
