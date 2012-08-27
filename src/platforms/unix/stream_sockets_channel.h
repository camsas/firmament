// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// UNIX stream sockets communication channel.

#ifndef FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H
#define FIRMAMENT_PLATFORMS_UNIX_STREAM_SOCKETS_CHANNEL_H

#include <boost/asio.hpp>

#include <string>
#include <vector>

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
  public boost::enable_shared_from_this<StreamSocketsChannel<T> >,
  private boost::noncopyable {
 public:
  typedef enum {
    SS_TCP = 0,
    SS_UNIX = 1
  } StreamSocketType;
  typedef StreamSocketsChannel<T> type;
  typedef shared_ptr<type> ptr_type;

  explicit StreamSocketsChannel(StreamSocketType type);
  explicit StreamSocketsChannel(TCPConnection::connection_ptr connection);
  virtual ~StreamSocketsChannel();
  void Close();
  bool Establish(const string& endpoint_uri);
  bool Ready();
  bool RecvA(misc::Envelope<T>* message,
             typename AsyncRecvHandler<T>::type callback);
  bool RecvS(misc::Envelope<T>* message);
  bool SendS(const misc::Envelope<T>& message);
  bool SendA(const misc::Envelope<T>& message,
             typename AsyncSendHandler<T>::type callback);
  virtual ostream& ToString(ostream* stream) const;

 protected:
  void RecvASecondStage(const boost::system::error_code& error,
                        const size_t bytes_read,
                        Envelope<T>* final_envelope,
                        typename AsyncRecvHandler<T>::type final_callback);
  void RecvAThirdStage(const boost::system::error_code& error,
                       const size_t bytes_read, size_t message_size,
                       Envelope<T>* final_envelope,
                       typename AsyncRecvHandler<T>::type final_callback);

 private:
  // Async receive buffer data structures and lock
  boost::mutex async_recv_lock_;
  scoped_ptr<boost::asio::mutable_buffers_1> async_recv_buffer_;
  scoped_ptr<vector<char> > async_recv_buffer_vec_;
  // TCP and io_service data structures
  shared_ptr<boost::asio::io_service> client_io_service_;
  scoped_ptr<boost::asio::io_service::work> io_service_work_;
  shared_ptr<boost::asio::ip::tcp::socket> client_socket_;
  TCPConnection::connection_ptr client_connection_;
  bool channel_ready_;
  StreamSocketType type_;
};

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament

#endif  // FIRMAMENT_PLATFORMS_UNIX_MESSAGING_STREAMSOCKETS_H
