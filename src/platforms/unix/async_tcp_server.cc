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

// Implementation of asynchronous TCP server class.

#include "platforms/unix/async_tcp_server.h"

#include <utility>
#include <boost/version.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/lexical_cast.hpp>

#include "misc/map-util.h"
#include "platforms/unix/tcp_connection.h"

using boost::asio::ip::tcp;
using boost::posix_time::ptime;
using boost::posix_time::second_clock;
using boost::posix_time::seconds;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

AsyncTCPServer::AsyncTCPServer(
    const string& endpoint_addr, const string& port,
    AcceptHandler::type accept_callback)
    : listening_(false),
        listening_interface_(""),
      accept_handler_(accept_callback),
      io_service_(new boost::asio::io_service),
      acceptor_(*io_service_) {
  VLOG(2) << "AsyncTCPServer starting!";
  tcp::resolver resolver(*io_service_);
  if (endpoint_addr == "") {
    LOG(FATAL) << "No endpoint address specified to listen on!";
  }
  tcp::resolver::query query(endpoint_addr, port);
  tcp::endpoint endpoint = *resolver.resolve(query);

  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();

  tcp::endpoint endp =  acceptor_.local_endpoint();
  listening_interface_ = "tcp:" + endp.address().to_string() + ":"
      + boost::lexical_cast<string>(endp.port());

  VLOG(1) << "Async TCP server listening on: " << listening_interface_;

  StartAccept();
}

void AsyncTCPServer::StartAccept() {
  VLOG(2) << "In StartAccept()";
  shared_ptr<tcp::endpoint> remote_endpoint(new tcp::endpoint());
  TCPConnection::connection_ptr new_connection(new TCPConnection(io_service_));
  acceptor_.async_accept(*new_connection->socket(),
                         *remote_endpoint,
                         boost::bind(&AsyncTCPServer::HandleAccept,
                                     this,
                                     new_connection,
                                     boost::asio::placeholders::error,
                                     remote_endpoint));
  listening_ = true;
}

void AsyncTCPServer::DropConnectionForEndpoint(const string& remote_endpoint) {
  endpoint_connection_map_.erase(remote_endpoint);
}

void AsyncTCPServer::Run() {
  // TODO(malte): Figure out if we need to reset the io_service itself here,
  // given that it may have been stopped beforehand.
  VLOG(2) << "Creating IO service thread";
  io_service_work_.reset(new boost::asio::io_service::work(*io_service_));
  thread_.reset(new boost::thread(
      boost::bind(&boost::asio::io_service::run, io_service_.get())));
  // Wait for thread to exit
  VLOG(2) << "IO service thread (" << thread_->get_id()
          << ") running -- Waiting for join...";
  thread_->join();
  VLOG(2) << "IO service terminated; TCP server's Run() method returning...";
}

AsyncTCPServer::~AsyncTCPServer() {
  VLOG(2) << "Async TCP server destructor called.";
  if (listening_)
    Stop();
}

void AsyncTCPServer::Stop() {
  // no-op if we are not listening -- there is nothing to stop
  if (!listening())
    return;

  listening_ = false;
  VLOG(2) << "Terminating " << endpoint_connection_map_.size()
          << " active TCP connections.";
  // This check around the close() call is necessary because ASIO does NOT
  // sanity-check if a socket is actually open before closing the underlying FD.
  // As a result, nasty races that lead to closing random FDs can occur if we
  // indiscriminately invoke close() here; see CL #241942.
  if (acceptor_.is_open())
    acceptor_.close();
  for (unordered_map<string, TCPConnection::connection_ptr>::iterator
       c_iter = endpoint_connection_map_.begin();
       c_iter != endpoint_connection_map_.end();
       ++c_iter) {
    c_iter->second->Close();
  }
  if (io_service_) {
    io_service_work_.reset();
    VLOG(2) << "Calling io_service.stop() from thread "
            << boost::this_thread::get_id();
    io_service_->stop();
  }
}

void AsyncTCPServer::HandleAccept(TCPConnection::connection_ptr connection,
                                  const boost::system::error_code& error,
                                  shared_ptr<tcp::endpoint> remote_endpoint) {
  if (!error) {
    VLOG(2) << "In HandleAccept, thread is " << boost::this_thread::get_id()
            << ", starting connection with IP " << remote_endpoint->address();
    connection->Start(remote_endpoint);
    // Get string version of remote endpoint
    string remote_ept_str = EndpointToString(*remote_endpoint);
    // Check we do not already have a connection for this endpoint
    CHECK(!endpoint_connection_map_.count(remote_ept_str));
    // Record a mapping for the connection's endpoint
    InsertIfNotPresent(&endpoint_connection_map_, remote_ept_str, connection);
    // Once the connection is up, we invoke the callback to notify the messaging
    // adapter (which will wrap the connection into a channel).
    VLOG(2) << "Invoking accept handler...";
    accept_handler_(connection);
    // Call StartAccept again to accept further connections.
    StartAccept();
  } else {
    LOG(ERROR) << "Error accepting socket connection. Error reported: "
               << error.message();
    return;
  }
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament
