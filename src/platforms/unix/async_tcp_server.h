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
#include "platforms/unix/tcp_connection.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Asynchronous, multi-threaded TCP server.
// Design inspired by
// http://www.boost.org/doc/html/boost_asio/example/http/server3/server.hpp.
class AsyncTCPServer : public boost::enable_shared_from_this<AsyncTCPServer>,
  private boost::noncopyable {
 public:
  AsyncTCPServer(const string& endpoint_addr, const string& port,
                 AcceptHandler::type accept_callback);
  ~AsyncTCPServer();
  void DropConnectionForEndpoint(const string& remote_endpoint);
  void Run();
  void Stop();
  TCPConnection::connection_ptr connection(const string& endpoint) {
    CHECK_EQ(endpoint_connection_map_.count(endpoint), 1);
    return endpoint_connection_map_[endpoint];
  }
  inline bool listening() { return listening_; }
  inline string listening_interface() { return listening_interface_; }
 private:
  bool listening_;
  string listening_interface_;
  void StartAccept();
  void HandleAccept(TCPConnection::connection_ptr connection,
                    const boost::system::error_code& error,
                    shared_ptr<tcp::endpoint> remote_endpoint);

  unordered_map<string, TCPConnection::connection_ptr> endpoint_connection_map_;
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
