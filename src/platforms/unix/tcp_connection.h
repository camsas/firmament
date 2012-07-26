// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// TCP connection class.

#ifndef FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H
#define FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H

#include <boost/asio.hpp>

#include <string>

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
  tcp::socket* socket() {
    return &socket_;
  }
  bool Ready() { return ready_; }
  void Start();
  void Send();
 private:
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
  tcp::socket socket_;
  TestMessage message_;
  bool ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_TCP_CONNECTION_H
