// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets-based messaging adapter.

#ifndef FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
#define FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H

#include <string>
#include <vector>
#include <map>

#include <boost/asio.hpp>
#include <boost/noncopyable.hpp>
#include <boost/enable_shared_from_this.hpp>

// This needs to go above other includes as it defines __PLATFORM_UNIX__
#include "platforms/unix/common.h"

#include "base/common.h"
#include "messages/base_message.pb.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/tcp_connection.h"
#include "platforms/unix/async_tcp_server.h"
#include "platforms/unix/stream_sockets_channel.h"

namespace firmament {
namespace platform_unix {
namespace streamsockets {

using boost::shared_ptr;

class TCPConnection;  // forward declaration
class AsyncTCPServer;  // forward declaration
template <class T>
class StreamSocketsChannel;  // forward declaration

// Messaging adapter.
class StreamSocketsMessaging : public firmament::MessagingInterface,
  public boost::enable_shared_from_this<StreamSocketsMessaging>,
  private boost::noncopyable {
 public:
  StreamSocketsMessaging() : message_wait_ready_(false), tcp_server_(NULL) { }
  virtual ~StreamSocketsMessaging();
  void AwaitNextMessage();
  void AddChannelForConnection(TCPConnection::connection_ptr connection);
  template <typename T>
  void CloseChannel(MessagingChannelInterface<T>* chan);
  template <typename T>
  bool EstablishChannel(const string& endpoint_uri,
                        MessagingChannelInterface<T>* chan);
  //template <typename T>
  shared_ptr<StreamSocketsChannel<BaseMessage> > GetChannelForConnection(
      uint64_t connection_id);
  void Listen(const string& endpoint_uri);
  bool ListenReady();
  //void SendOnConnection(uint64_t connection_id);
  void StopListen();

  //template <typename T>
  const vector<shared_ptr<StreamSocketsChannel<BaseMessage> > >&
      active_channels() {
    return active_channels_;
  }

 private:
  void HandleAsyncMessageRecv(
      const boost::system::error_code& error, size_t bytes_transferred,
      shared_ptr<StreamSocketsChannel<BaseMessage> > chan);

  AsyncTCPServer* tcp_server_;
  vector<shared_ptr<StreamSocketsChannel<BaseMessage> > > active_channels_;
  map<shared_ptr<StreamSocketsChannel<BaseMessage> >, Envelope<BaseMessage>* >
      channel_recv_envelopes_;
  // Synchronization variables, locks tec.
  boost::mutex message_wait_mutex_;
  boost::condition_variable message_wait_condvar_;
  bool message_wait_ready_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
