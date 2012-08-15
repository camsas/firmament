// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H

#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"
#include "platforms/unix/stream_sockets_channel.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

class TCPConnection;  // forward declaration
class AsyncTCPServer;  // forward declaration
template <class T>
class StreamSocketsChannel;  // forward declaration

// Messaging adapter.
class StreamSocketsMessaging : public firmament::MessagingInterface,
  public boost::enable_shared_from_this<StreamSocketsMessaging>,
  private boost::noncopyable {
 public:
  const vector<boost::shared_ptr<StreamSocketsChannel<BaseMessage> > >&
      active_channels() {
    return active_channels_;
  }
  virtual ~StreamSocketsMessaging();
  BaseMessage* AwaitNextMessage();
  void AddChannelForConnection(TCPConnection::connection_ptr connection);
  template <class T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  template <class T>
  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan);
  boost::shared_ptr<StreamSocketsChannel<BaseMessage> > GetChannelForConnection(
      uint64_t connection_id);
  void Listen(const string& endpoint_uri);
  bool ListenReady();
  void SendOnConnection(uint64_t connection_id);
  void StopListen();

 private:
  AsyncTCPServer* tcp_server_;
  vector<boost::shared_ptr<StreamSocketsChannel<BaseMessage> > >
      active_channels_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
