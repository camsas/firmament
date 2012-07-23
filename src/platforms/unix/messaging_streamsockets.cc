// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of streaming sockets-based messaging adapter.

#include "platforms/unix/messaging_streamsockets.h"

#include <boost/thread.hpp>
#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>

using boost::asio::ip::tcp;

namespace firmament {
namespace platform_unix {
namespace streamsockets {

// ----------------------------
// StreamSocketsMessaging
// ----------------------------

StreamSocketsMessaging::~StreamSocketsMessaging() {
}

Message* StreamSocketsMessaging::AwaitNextMessage() {
  LOG(FATAL) << "Unimplemented!";
  return NULL;
}

void StreamSocketsMessaging::Listen(const string& endpoint_uri) {
  // Parse endpoint URI into hostname and port
  string hostname = URITools::GetHostnameFromURI(endpoint_uri);
  string port = URITools::GetPortFromURI(endpoint_uri);

  VLOG(1) << "Creating an async TCP server on port " << port
          << " on endpoint " << hostname << "(" << endpoint_uri << ")";
  tcp_server_ = new AsyncTCPServer(hostname, port);
  boost::thread t(boost::bind(&AsyncTCPServer::Run, tcp_server_));
  VLOG(1) << "Binding AsyncTCPServer::Run to thread " << t.get_id();
}

bool StreamSocketsMessaging::ListenReady() {
  if (tcp_server_ != NULL)
    return tcp_server_->listening();
  else
    return false;
}

void StreamSocketsMessaging::SendOnConnection(uint64_t connection_id) {
  VLOG(2) << "Messaging adapter sending on connection " << connection_id;
  // TODO(malte): Hack -- we spin until the connection is ready. This is
  // required to avoid race conditions where a messaging adapter is trying to
  // send on a connection before it is ready. This can occur due to the
  // asynchronous, multi-threaded nature of the TCP server.
  while (!tcp_server_->connection(connection_id)->Ready()) {
    VLOG(2) << "Waiting for connection " << connection_id
            << " to be ready to send...";
  }
  // Actually send the data on the (now ready) TCP connection
  tcp_server_->connection(connection_id)->Send();
}

void StreamSocketsMessaging::StopListen() {
  tcp_server_->Stop();
  // t.join()
}

}  // namespace streamsockets
}  // namespace platform_unix
}  // namespace firmament
