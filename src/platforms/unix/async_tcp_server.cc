// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of asynchronous TCP server class.

#include "platforms/unix/async_tcp_server.h"

#include <utility>
#include <boost/version.hpp>
#include <boost/thread.hpp>
#include <boost/bind.hpp>

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
  acceptor_.close();
  for (map<shared_ptr<tcp::endpoint>, TCPConnection::connection_ptr>::iterator
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
    connection->Start();
    // Check we do not already have a connection for this endpoint
    CHECK(!endpoint_connection_map_.count(remote_endpoint));
    // Record a mapping for the connection's endpoint
    endpoint_connection_map_.insert(
        pair<shared_ptr<tcp::endpoint>, TCPConnection::connection_ptr>(
            remote_endpoint, connection));
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
