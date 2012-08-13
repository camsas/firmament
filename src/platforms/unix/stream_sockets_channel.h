// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H

#include <boost/asio.hpp>

#include <string>

#include <boost/noncopyable.hpp>

#include "base/common.h"
#include "misc/envelope.h"
#include "misc/protobuf_envelope.h"
#include "misc/messaging_interface.h"
#include "misc/uri_tools.h"
#include "platforms/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// Channel.
template <class T>
class StreamSocketsChannel : public MessagingChannelInterface<T>,
                             private boost::noncopyable {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;
  explicit StreamSocketsChannel(StreamSocketType type);
  explicit StreamSocketsChannel(tcp::socket* socket);
  virtual ~StreamSocketsChannel();
  void Close();
  bool Establish(const string& endpoint_uri);
  bool Ready();
  bool RecvA(misc::Envelope<T>* message);
  bool RecvS(misc::Envelope<T>* message);
  bool SendS(const misc::Envelope<T>& message);
  bool SendA(const misc::Envelope<T>& message);
  virtual ostream& ToString(ostream* stream) const;

 private:
  boost::shared_ptr<boost::asio::io_service> client_io_service_;
  boost::shared_ptr<boost::asio::ip::tcp::socket> client_socket_;
  bool channel_ready_;
  StreamSocketType type_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
