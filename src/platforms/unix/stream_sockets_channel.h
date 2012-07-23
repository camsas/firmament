// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H

#include <boost/asio.hpp>

#include <string>

#include <boost/noncopyable.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Channel.
template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T> {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;
  explicit StreamSocketsChannel(StreamSocketType type);
  virtual ~StreamSocketsChannel();
  void Close();
  void Establish(const string& endpoint_uri);
  bool Ready();
  T* RecvA();
  bool RecvS(T* message);
  void Send(const T& message);

 private:
  boost::asio::io_service client_io_service_;
  tcp::socket* client_socket_;
  bool channel_ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
