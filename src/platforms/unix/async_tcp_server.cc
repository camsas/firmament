// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of asynchronous TCP server class.

#include "platforms/unix/async_tcp_server.h"

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
    shared_ptr<StreamSocketsMessaging> messaging_adapter)
    : acceptor_(io_service_), listening_(false),
      owning_adapter_(messaging_adapter) {
  VLOG(2) << "AsyncTCPServer starting!";
  tcp::resolver resolver(io_service_);
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
  TCPConnection::connection_ptr new_connection(new TCPConnection(io_service_));
  active_connections_.push_back(new_connection);
  acceptor_.async_accept(*new_connection->socket(),
                         boost::bind(&AsyncTCPServer::HandleAccept, this,
                                     new_connection,
                                     boost::asio::placeholders::error));
  listening_ = true;
}

void AsyncTCPServer::Run() {
  VLOG(2) << "Creating IO service thread";
  boost::shared_ptr<boost::thread> thread(new boost::thread(
      boost::bind(&boost::asio::io_service::run, &io_service_)));
  // Wait for thread to exit
  VLOG(2) << "IO service thread (" << thread->get_id()
          << ") running -- Waiting for join...";
  thread->join();
}

void AsyncTCPServer::Stop() {
  listening_ = false;
  VLOG(2) << "Terminating " << active_connections_.size()
          << " active TCP connections.";
  active_connections_.clear();
  /*for (vector<TCPConnection::connection_ptr>::const_iterator conn_iter =
       active_connections_.begin();
       conn_iter < active_connections_.end();
       ++conn_iter) {
  }*/
  acceptor_.close();
  io_service_.stop();
#if (BOOST_VERSION >= 104700)
  while (!io_service_.stopped()) { }  // spin until stopped
#else
  uint32_t wait_seconds = 2;
  VLOG(1) << "Waiting " << wait_seconds
          << " seconds for io_service to shut down...";
  boost::this_thread::sleep(boost::posix_time::seconds(wait_seconds));
#endif
  io_service_.~io_service();
}

void AsyncTCPServer::HandleAccept(TCPConnection::connection_ptr connection,
                                  const boost::system::error_code& error) {
  if (!error) {
    VLOG(2) << "In HandleAccept, thread is " << boost::this_thread::get_id()
            << ", starting connection";
    connection->Start();
    // Once the connection is up, we wrap it into a channel.
    owning_adapter_->AddChannelForConnection(connection);
    // Call StartAccept again to accept further connections.
    StartAccept();
  } else {
    LOG(FATAL) << "Error accepting socket connection. Error reported: "
               << error.message();
    return;
  }
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament
