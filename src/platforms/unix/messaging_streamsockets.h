// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H

#include <boost/asio.hpp>

#include <iostream>
#include <string>
#include <sstream>

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"

using boost::asio::ip::tcp;

namespace firmament {

class TCPConnection : public boost::enable_shared_from_this<TCPConnection>,
                      private boost::noncopyable {
 public:
  typedef boost::shared_ptr<TCPConnection> connection_ptr;
  explicit TCPConnection(boost::asio::io_service& io_service)
      : socket_(io_service) { }
  tcp::socket& socket() {
    return socket_;
  }
  void Start();
 private:
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  tcp::socket socket_;
  std::string message_;
};

// Asynchronous, multi-threaded TCP server.
// Design inspired by
// http://www.boost.org/doc/html/boost_asio/example/http/server3/server.hpp.
class AsyncTCPServer : private boost::noncopyable {
 public:
  explicit AsyncTCPServer(string endpoint_addr, uint32_t port);
  void Run();
  void Stop();
 private:
  void StartAccept();
  void HandleAccept(const boost::system::error_code& error);
  boost::asio::io_service io_service_;
  tcp::acceptor acceptor_;
  TCPConnection::connection_ptr new_connection_;
};

class StreamSocketsMessaging : public MessagingInterface {
 public:
  template <class T>
  void EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan) {
    VLOG(1) << "got here, endpoint is " << endpoint_uri << ", chan: " << chan
            << "!";

  }
  void Listen(uint64_t port) {
    VLOG(1) << "Listening on port " << port;
  }

  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  Message* AwaitNextMessage();
};

template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T> {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;

  void EstablishChannel(const string& endpoint_uri, uint32_t port) {
    VLOG(1) << "got here, endpoint is " << endpoint_uri;
    tcp::resolver resolver(client_io_service_);
    stringstream port_ss;
    port_ss << port;
    tcp::resolver::query query(endpoint_uri, port_ss.str());
    tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
    tcp::resolver::iterator end;

    client_socket_ = new tcp::socket(client_io_service_);
    boost::system::error_code error = boost::asio::error::host_not_found;
    while (error && endpoint_iterator != end) {
      client_socket_->close();
      client_socket_->connect(*endpoint_iterator++, error);
    }
    if (error)
      throw boost::system::system_error(error);
    else
      VLOG(2) << "we appear to have connected successfully...";
  }

  void Listen(string endpoint_addr, uint64_t port) {
    VLOG(1) << "Creating an async TCP server on port " << port;
    tcp_server_ = new AsyncTCPServer(endpoint_addr, port);
    boost::thread t(boost::bind(&AsyncTCPServer::Run, tcp_server_));
  }

  StreamSocketsChannel(StreamSocketType type) : client_socket_(NULL) {
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
  virtual ~StreamSocketsChannel() {
    if (client_socket_ != NULL)
      delete client_socket_;
  }
  // Send (sync?)
  void Send(const T& message) {
    VLOG(1) << "Trying to send message: " << &message;
  }
  // Synchronous receive -- blocks until the next message is received.
  T* RecvS() {
    VLOG(2) << "In RecvS, polling for data";
    for (;;) {
      boost::array<char, 128> buf;
      boost::system::error_code error;
      VLOG(2) << "calling read_some";
      size_t len = client_socket_->read_some(boost::asio::buffer(buf), error);
      VLOG(2) << "Read " << len << " bytes...";

      if (error == boost::asio::error::eof) {
        VLOG(2) << "Received EOF, connection terminating!";
        break;
      } else if (error) {
        throw boost::system::system_error(error);
      }

      cout.write(buf.data(), len);
    }
    return NULL;
  }
  // Asynchronous receive -- does not block.
  T* RecvA() { return NULL; }
 private:
  boost::asio::io_service client_io_service_;
  AsyncTCPServer* tcp_server_;
  tcp::socket* client_socket_;
};

}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
