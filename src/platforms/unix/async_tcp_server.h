// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Asynchronous TCP server class.

#ifndef FIRMAMENT_PLATFORMS_UNIX_ASYNC_TCP_SERVER_H
#define FIRMAMENT_PLATFORMS_UNIX_ASYNC_TCP_SERVER_H

#include <boost/asio.hpp>

#include <string>
#include <vector>
#include <map>

#include <boost/bind.hpp>
#include <boost/noncopyable.hpp>
#include <boost/thread.hpp>

#include "base/common.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/messaging_streamsockets.h"
#include "platforms/unix/tcp_connection.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Forward declaration
template <typename T>
class StreamSocketsMessaging;

// Asynchronous, multi-threaded TCP server.
// Design inspired by
// http://www.boost.org/doc/html/boost_asio/example/http/server3/server.hpp.
class AsyncTCPServer : public boost::enable_shared_from_this<AsyncTCPServer>,
  private boost::noncopyable {
 public:
  AsyncTCPServer(const string& endpoint_addr, const string& port,
                 AcceptHandler::type accept_callback);
  ~AsyncTCPServer();
  void Run();
  void Stop();
  TCPConnection::connection_ptr connection(
      const shared_ptr<tcp::endpoint> endpoint) {
    CHECK_EQ(endpoint_connection_map_.count(endpoint), 1);
    return endpoint_connection_map_[endpoint];
  }
  inline bool listening() { return listening_; }
 private:
  bool listening_;
  void StartAccept();
  void HandleAccept(TCPConnection::connection_ptr connection,
                    const boost::system::error_code& error,
                    shared_ptr<tcp::endpoint> remote_endpoint);

  map<boost::shared_ptr<tcp::endpoint>, TCPConnection::connection_ptr>
      endpoint_connection_map_;
  AcceptHandler::type accept_handler_;
  scoped_ptr<boost::thread> thread_;
  scoped_ptr<boost::asio::io_service::work> io_service_work_;
  shared_ptr<boost::asio::io_service> io_service_;
  tcp::acceptor acceptor_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_ASYNC_TCP_SERVER_H
