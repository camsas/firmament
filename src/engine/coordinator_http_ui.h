// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
#define FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "engine/coordinator.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets.h"

#include <pion/net/HTTPServer.hpp>
#include <pion/net/HTTPTypes.hpp>
#include <pion/net/HTTPRequest.hpp>
#include <pion/net/HTTPResponseWriter.hpp>

namespace firmament {

using pion::net::HTTPServerPtr;
using pion::net::HTTPRequestPtr;
using pion::net::TCPConnectionPtr;

// Forward declaration
class Coordinator;

class CoordinatorHTTPUI {
 public:
  explicit CoordinatorHTTPUI(Coordinator *coordinator);
  virtual ~CoordinatorHTTPUI();
  void init(uint32_t port);
  void handleRootURI(HTTPRequestPtr& http_request,  // NOLINT
                     TCPConnectionPtr& tcp_conn);
 protected:
  HTTPServerPtr coordinator_http_server_;
  Coordinator *coordinator_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
