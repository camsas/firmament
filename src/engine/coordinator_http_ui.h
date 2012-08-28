// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
#define FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H

#include "base/common.h"
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "engine/coordinator.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"

#include <pion/net/HTTPServer.hpp>
#include <pion/net/HTTPTypes.hpp>
#include <pion/net/HTTPRequest.hpp>
#include <pion/net/HTTPResponseWriter.hpp>

namespace firmament {

// Forward declaration
class Coordinator;

namespace webui {

using pion::net::HTTPServerPtr;
using pion::net::HTTPRequestPtr;
using pion::net::TCPConnectionPtr;
using pion::net::HTTPResponseWriterPtr;

const string kHTMLStart = "<html><body>\n";
const string kHTMLEnd = "</body></html>\n";

class CoordinatorHTTPUI {
 public:
  explicit CoordinatorHTTPUI(shared_ptr<Coordinator> coordinator);
  virtual ~CoordinatorHTTPUI();
  void FinishOkResponse(HTTPResponseWriterPtr writer);
  void Init(uint16_t port);
  HTTPResponseWriterPtr InitOkResponse(HTTPRequestPtr http_request,
                                       TCPConnectionPtr tcp_conn);
  void LogRequest(HTTPRequestPtr& http_request);
  void HandleRootURI(HTTPRequestPtr& http_request,  // NOLINT
                     TCPConnectionPtr& tcp_conn);
  void HandleResourcesURI(HTTPRequestPtr& http_request,  // NOLINT
                          TCPConnectionPtr& tcp_conn);
  void HandleInjectURI(HTTPRequestPtr& http_request,  // NOLINT
                       TCPConnectionPtr& tcp_conn);
  void HandleShutdownURI(HTTPRequestPtr& http_request,  // NOLINT
                         TCPConnectionPtr& tcp_conn);

 protected:
  HTTPServerPtr coordinator_http_server_;
  shared_ptr<Coordinator> coordinator_;
};

}  // namespace webui
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
