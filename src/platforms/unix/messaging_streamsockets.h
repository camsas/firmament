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
#include "misc/uri_tools.h"
#include "platforms/common.h"

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// TCP connection class using boost primitives.
class TCPConnection : public boost::enable_shared_from_this<TCPConnection>,
                      private boost::noncopyable {
 public:
  typedef boost::shared_ptr<TCPConnection> connection_ptr;
  explicit TCPConnection(boost::asio::io_service& io_service)
      : socket_(io_service), ready_(false) { }
  virtual ~TCPConnection();
  tcp::socket& socket() {
    return socket_;
  }
  bool Ready() { return ready_; }
  void Start();
  void Send();
 private:
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  tcp::socket socket_;
  std::string message_;
  bool ready_;
};

// Asynchronous, multi-threaded TCP server.
// Design inspired by
// http://www.boost.org/doc/html/boost_asio/example/http/server3/server.hpp.
class AsyncTCPServer : private boost::noncopyable {
 public:
  explicit AsyncTCPServer(const string& endpoint_addr, const string& port);
  void Run();
  void Stop();
  TCPConnection::connection_ptr connection(uint64_t connection_id) {
    CHECK_LT(connection_id, active_connections_.size());
    return active_connections_[connection_id];
  }
  inline bool listening() { return listening_; }
 private:
  bool listening_;
  void StartAccept();
  void HandleAccept(TCPConnection::connection_ptr connection,
                    const boost::system::error_code& error);
  boost::asio::io_service io_service_;
  tcp::acceptor acceptor_;
  vector<TCPConnection::connection_ptr> active_connections_;
};

// Messaging adapter.
class StreamSocketsMessaging : public firmament::MessagingInterface {
 public:
  virtual ~StreamSocketsMessaging() {};
  template <class T>
  void EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan) {
    VLOG(1) << "got here, endpoint is " << endpoint_uri << ", chan: " << chan
            << "!";
    chan->Establish(endpoint_uri);
  }

  void Listen(const string& endpoint_uri) {
    // Parse endpoint URI into hostname and port
    string hostname = URITools::GetHostnameFromURI(endpoint_uri);
    string port = URITools::GetPortFromURI(endpoint_uri);

    VLOG(1) << "Creating an async TCP server on port " << port
            << " on endpoint " << hostname << "(" << endpoint_uri << ")";
    tcp_server_ = new AsyncTCPServer(hostname, port);
    boost::thread t(boost::bind(&AsyncTCPServer::Run, tcp_server_));
  }

  bool ListenReady() {
    if (tcp_server_ != NULL)
      return tcp_server_->listening();
    else
      return false;
  }

  void SendOnConnection(uint64_t connection_id) {
    VLOG(2) << "Messaging adapter sending on connection " << connection_id;
    // TODO(malte): Hack -- we spin until the connection is ready. This is
    // required to avoid race conditions where a messaging adapter is trying to
    // send on a connection before it is ready. This can occur due to the
    // asynchronous, multi-threaded nature of the TCP server.
    while (!tcp_server_->connection(connection_id)->Ready()) {
      VLOG(2) << "Waiting for connection " << connection_id
              << " to be ready to send...";
    }
    // Actually send the data on the (now ready) TCP connection
    tcp_server_->connection(connection_id)->Send();
  }

  void StopListen() {
    // t.stop()
    // t.join()
  }

  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);

  Message* AwaitNextMessage();

 private:
  AsyncTCPServer* tcp_server_;
};

// Channel.
template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T> {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;

  void Establish(const string& endpoint_uri) {
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

  StreamSocketsChannel(StreamSocketType type) : client_socket_(NULL),
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
  virtual ~StreamSocketsChannel() {
    if (client_socket_ != NULL)
      delete client_socket_;
  }
  // Ready check
  bool Ready() {
    VLOG(2) << "check if channel ready: is " << channel_ready_;
    return channel_ready_;
  }
  // Send (sync?)
  void Send(const T& message) {
    VLOG(1) << "Trying to send message: " << &message;
  }
  // Synchronous receive -- blocks until the next message is received.
  T* RecvS() {
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
  T* RecvA() { return NULL; }
  void Close() {
    // end the connection
    client_socket_->close();
    channel_ready_ = false;
  }
 private:
  boost::asio::io_service client_io_service_;
  tcp::socket* client_socket_;
  bool channel_ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
