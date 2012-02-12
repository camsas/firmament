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

/*template <class T>
void StreamSocketsMessaging::EstablishChannel(
    const string& endpoint_uri,
    MessagingChannelInterface<T>* chan) {
  VLOG(1) << "got here, endpoint is " << endpoint_uri << ", chan: " << chan
          << "!";
}*/

Message* StreamSocketsMessaging::AwaitNextMessage() {
  LOG(FATAL) << "Unimplemented!";
  return NULL;
}


void TCPConnection::Start() {
  // XXX: this needs to change, of course
  message_ = "Hello world!\n";
  VLOG(3) << "Sending message in server...";
  boost::asio::async_write(
      socket_, boost::asio::buffer(message_),
      boost::bind(&TCPConnection::HandleWrite, shared_from_this(),
                  boost::asio::placeholders::error,
                  boost::asio::placeholders::bytes_transferred));
}

void TCPConnection::HandleWrite(const boost::system::error_code& error,
                                size_t bytes_transferred) {
  VLOG(3) << "In HandleWrite, transferred " << bytes_transferred << " bytes.";
}



AsyncTCPServer::AsyncTCPServer(string endpoint_addr, uint32_t port)
    : acceptor_(io_service_), new_connection_(new TCPConnection(io_service_)) {
  VLOG(2) << "AsyncTCPServer starting!";
  tcp::resolver resolver(io_service_);
  stringstream port_ss;
  port_ss << port;
  if (endpoint_addr == "") {
    LOG(FATAL) << "No endpoint address specified to listen on!";
  }
  tcp::resolver::query query(endpoint_addr, port_ss.str());
  tcp::endpoint endpoint = *resolver.resolve(query);

  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(true));
  acceptor_.bind(endpoint);
  acceptor_.listen();
  StartAccept();
}

void AsyncTCPServer::StartAccept() {
  VLOG(2) << "In StartAccept()";
  acceptor_.async_accept(new_connection_->socket(),
                         boost::bind(&AsyncTCPServer::HandleAccept, this,
                                     boost::asio::placeholders::error));
}

void AsyncTCPServer::Run() {
  VLOG(2) << "Creating TCP server thread";
  boost::shared_ptr<boost::thread> thread(new boost::thread(
      boost::bind(&boost::asio::io_service::run, &io_service_)));
  // Wait for thread to exit
  VLOG(2) << "Waiting for join...";
  thread->join();
}

void AsyncTCPServer::Stop() {
  io_service_.stop();
}

void AsyncTCPServer::HandleAccept(const boost::system::error_code& error) {
  if (!error) {
    VLOG(2) << "In HandleAccept -- starting connection";
    new_connection_->Start();
    new_connection_.reset(new TCPConnection(io_service_));
    StartAccept();
  } else {
    VLOG(1) << "Error: " << error;
    return;
  }
}

}  // namespace firmament
