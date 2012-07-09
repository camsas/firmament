// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_INL_H

#include "platforms/unix/stream_sockets_channel.h"

#include <boost/asio.hpp>

#include <string>

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// ----------------------------
// StreamSocketsChannel
// ----------------------------

template <class T>
StreamSocketsChannel<T>::StreamSocketsChannel(StreamSocketType type)
  : client_socket_(NULL),
    channel_ready_(false) {
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
StreamSocketsChannel<T>::~StreamSocketsChannel() {
  if (client_socket_ != NULL)
    delete client_socket_;
}

template <class T>
void StreamSocketsChannel<T>::Establish(const string& endpoint_uri) {
  // Parse endpoint URI into hostname and port
  string hostname = URITools::GetHostnameFromURI(endpoint_uri);
  string port = URITools::GetPortFromURI(endpoint_uri);

  // Now make the connection
  VLOG(1) << "got here, endpoint is " << endpoint_uri;
  tcp::resolver resolver(client_io_service_);
  tcp::resolver::query query(hostname, port);
  tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
  tcp::resolver::iterator end;

  client_socket_ = new tcp::socket(client_io_service_);
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
  VLOG(2) << "check if channel ready: is " << channel_ready_;
  return channel_ready_;
}

// Send (sync?)
template <class T>
void StreamSocketsChannel<T>::Send(const T& message) {
  VLOG(1) << "Trying to send message: " << &message;
}

// Synchronous recieve -- blocks until the next message is received.
template <class T>
T* StreamSocketsChannel<T>::RecvS() {
  VLOG(2) << "In RecvS, polling for data";
  boost::array<char, 1024> buf;
  size_t len;
  do {
    boost::system::error_code error;
    VLOG(3) << "calling read_some";
    len = client_socket_->read_some(boost::asio::buffer(buf), error);
    VLOG(2) << "Read " << len << " bytes...";

    if (error == boost::asio::error::eof) {
      VLOG(2) << "Received EOF, connection terminating!";
      break;
    } else if (error) {
      throw boost::system::system_error(error);
    }

    // DEBUG
    cout.write(buf.data(), len);
  } while (client_socket_->available() > 0);
  //static_cast<void*>(buffer) = buf.data();
  return static_cast<T*>(static_cast<void*>(buf.data()));
}

// Asynchronous receive -- does not block.
template <class T>
T* StreamSocketsChannel<T>::RecvA() {
  LOG(FATAL) << "Unimplemented!";
  return NULL;
}

template <class T>
void StreamSocketsChannel<T>::Close() {
  // end the connection
  client_socket_->close();
  channel_ready_ = false;
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_INL_H
