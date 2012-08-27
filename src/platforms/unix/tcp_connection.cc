// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of TCP connection class.

#include "platforms/unix/tcp_connection.h"

#include <vector>

#include <boost/thread.hpp>
#include <boost/bind.hpp>

namespace firmament {
namespace platform_unix {
namespace streamsockets {

using boost::asio::ip::tcp;

TCPConnection::~TCPConnection() {
  if (Ready())
    Close();
  CHECK(!Ready());
  VLOG(2) << "Connection is being destroyed!";
}

void TCPConnection::Close() {
  VLOG(2) << "Closing TCP connection at " << this;
  ready_ = false;
  boost::system::error_code ec;
  socket_.shutdown(tcp::socket::shutdown_both, ec);
  if (ec)
    LOG(WARNING) << "Error shutting down connections on socket for "
                 << "connection at " << this << ": " << ec.message();
  // We do not currently call socket_.close() here, as this appears to happen
  // implicitly, and causes trouble if we do and then do not destroy the TCP
  // connection, which cannot recover from the closed socket currently.
}

void TCPConnection::Start() {
  VLOG(2) << "TCP connection starting!";
  ready_ = true;
  CHECK(socket_.is_open());
}

/*void TCPConnection::Send() {
  // XXX: this needs to change, of course
  VLOG(2) << "Sending message of length " << message_.ByteSize()
          << " in server...";
  size_t msg_size = message_.ByteSize();
  vector<char> buf(msg_size);
  CHECK(message_.SerializeToArray(&buf[0], message_.ByteSize()));
  // Send data size
  boost::asio::async_write(
      socket_, boost::asio::buffer(reinterpret_cast<char*>(&msg_size),
                                   sizeof(msg_size)),
      boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
  // Send the data
  boost::asio::async_write(
      socket_, boost::asio::buffer(buf, msg_size),
      boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}*/

void TCPConnection::HandleWrite(const boost::system::error_code& error,
                                size_t bytes_transferred) {
  if (error) {
    LOG(ERROR) << "Failed to write to socket. Error reported: " << error
               << ", bytes_transferred: " << bytes_transferred;
  } else {
    VLOG(2) << "In HandleWrite, transferred " << bytes_transferred << " bytes.";
  }
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament
