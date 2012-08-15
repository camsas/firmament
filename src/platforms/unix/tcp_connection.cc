// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of TCP connection class.

#include "platforms/unix/tcp_connection.h"

#include <vector>

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

TCPConnection::~TCPConnection() {
  VLOG(2) << "Connection is being destroyed!";
  socket_.close();
}

void TCPConnection::Start() {
  ready_ = true;
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
