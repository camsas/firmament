// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
  explicit TCPConnection(shared_ptr<io_service> io_service)  // NOLINT
      : io_service_(io_service), socket_(*io_service), ready_(false) { }
  virtual ~TCPConnection();
  // XXX(malte): unsafe raw pointer, fix this
  tcp::socket* socket() {
    return &socket_;
  }
  const string LocalEndpointString();
  bool Ready() { return ready_; }
  const string RemoteEndpointString();
  void Start();
  void Close();

 private:
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  tcp::socket socket_;
  shared_ptr<io_service> io_service_;
  bool ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H
