// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of streaming sockets-based messaging adapter.

#include "platforms/unix/messaging_streamsockets.h"

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

using boost::asio::ip::tcp;

namespace firmament {

Message* StreamSocketsMessaging::AwaitNextMessage() {
  LOG(FATAL) << "Unimplemented!";
  return NULL;
}

TCPConnection::~TCPConnection() {
  VLOG(2) << "Connection is being destroyed!";
}

void TCPConnection::Start() {
}

void TCPConnection::Send() {
  // XXX: this needs to change, of course
  message_ = "Hello world!\n";
  VLOG(2) << "Sending message in server...";
  boost::asio::async_write(
      socket_, boost::asio::buffer(message_),
      boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}

void TCPConnection::HandleWrite(const boost::system::error_code& error,
                                size_t bytes_transferred) {
  if (error) {
    VLOG(2) << "Error: " << error;
  } else {
    VLOG(2) << "In HandleWrite, transferred " << bytes_transferred << " bytes.";
  }
}



AsyncTCPServer::AsyncTCPServer(const string& endpoint_addr, const string& port)
    : acceptor_(io_service_), listening_(false) {
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
  acceptor_.async_accept(new_connection->socket(),
                         boost::bind(&AsyncTCPServer::HandleAccept, this,
                                     new_connection,
                                     boost::asio::placeholders::error));
  listening_ = true;
}

void AsyncTCPServer::Run() {
  VLOG(2) << "Creating TCP server thread";
  boost::shared_ptr<boost::thread> thread(new boost::thread(
      boost::bind(&boost::asio::io_service::run, &io_service_)));
  // Wait for thread to exit
  VLOG(2) << "Server thread running -- Waiting for join...";
  thread->join();
}

void AsyncTCPServer::Stop() {
  listening_ = false;
  io_service_.stop();
}

void AsyncTCPServer::HandleAccept(TCPConnection::connection_ptr connection,
                                  const boost::system::error_code& error) {
  if (!error) {
    VLOG(2) << "In HandleAccept -- starting connection at " << connection;
    connection->Start();
    StartAccept();
  } else {
    VLOG(1) << "Error: " << error;
    return;
  }
}

}  // namespace firmament
