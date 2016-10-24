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

// TCP connection class.

#ifndef FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H
#define FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H

#include <boost/asio.hpp>

#include <string>

#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

// This needs to go above other includes as it defines __PLATFORM_UNIX__
#include "platforms/unix/common.h"

#include "base/common.h"
#include "base/types.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

using boost::asio::io_service;
using boost::asio::ip::tcp;

// TCP connection class using boost primitives.
class TCPConnection : public boost::enable_shared_from_this<TCPConnection>,
                      private boost::noncopyable {
 public:
  typedef shared_ptr<TCPConnection> connection_ptr;
  explicit TCPConnection(shared_ptr<io_service> io_service)
      : socket_(*io_service), io_service_(io_service), ready_(false) { }
  virtual ~TCPConnection();
  // XXX(malte): unsafe raw pointer, fix this
  tcp::socket* socket() {
    return &socket_;
  }
  const string LocalEndpointString();
  bool Ready() { return ready_; }
  const string RemoteEndpointString();
  void Start(shared_ptr<tcp::endpoint> remote_endpoint);
  void Close();

 private:
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  tcp::socket socket_;
  shared_ptr<io_service> io_service_;
  shared_ptr<tcp::endpoint> remote_endpoint_;
  bool ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H
