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
  if (socket_.is_open())
    socket_.shutdown(tcp::socket::shutdown_both, ec);
  if (ec)
    LOG(WARNING) << "Error shutting down connections on socket for "
                 << "connection at " << this << ": " << ec.message();
  // We do not currently call socket_.close() here, as this appears to happen
  // implicitly, and causes trouble if we do and then do not destroy the TCP
  // connection, which cannot recover from the closed socket currently.
}

const string TCPConnection::LocalEndpointString() {
  if (!ready_)
    return "";
  boost::system::error_code ec;
  string ip_address = socket_.local_endpoint(ec).address().to_string();
  string port = to_string<uint16_t>(socket_.local_endpoint(ec).port());
  string endpoint = "tcp:" + ip_address + ":" + port;
  if (ec)
    return "";
  return endpoint;
}

const string TCPConnection::RemoteEndpointString() {
  if (!ready_)
    return "";
  boost::system::error_code ec;
  string ip_address = socket_.remote_endpoint(ec).address().to_string();
  string port = to_string<uint16_t>(socket_.remote_endpoint(ec).port());
  if (ec)
    return "";
  string endpoint = "tcp:" + ip_address + ":" + port;
  return endpoint;
}

void TCPConnection::Start(shared_ptr<tcp::endpoint> remote_endpoint) {
  VLOG(2) << "TCP connection starting!";
  ready_ = true;
  remote_endpoint_ = remote_endpoint;
  CHECK_EQ(*remote_endpoint, socket_.remote_endpoint());
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
