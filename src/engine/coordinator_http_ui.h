// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
#define FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H

#include <string>
#include <utility>

#include <pion/net/HTTPServer.hpp>
#include <pion/net/HTTPTypes.hpp>
#include <pion/net/HTTPRequest.hpp>
#include <pion/net/HTTPResponseWriter.hpp>
#include <ctemplate/template.h>

#include "base/common.h"
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "engine/coordinator.h"
#include "platforms/common.h"
#include "platforms/unix/stream_sockets_adapter.h"

namespace firmament {

// Forward declaration
class Coordinator;

namespace webui {

using pion::net::HTTPServerPtr;
using pion::net::HTTPRequestPtr;
using pion::net::TCPConnectionPtr;
using pion::net::HTTPResponseWriterPtr;

using ctemplate::TemplateDictionary;

class CoordinatorHTTPUI {
 public:
  explicit CoordinatorHTTPUI(shared_ptr<Coordinator> coordinator);
  virtual ~CoordinatorHTTPUI();
  void ErrorResponse(const unsigned int error_code,
                     HTTPRequestPtr http_request,
                     TCPConnectionPtr tcp_conn);
  void FinishOkResponse(HTTPResponseWriterPtr writer);
  void Init(uint16_t port);
  HTTPResponseWriterPtr InitOkResponse(HTTPRequestPtr http_request,
                                       TCPConnectionPtr tcp_conn);
  void LogRequest(const HTTPRequestPtr& http_request);
  void HandleFaviconURI(HTTPRequestPtr& http_request,  // NOLINT
                        TCPConnectionPtr& tcp_conn);
  void HandleJobURI(HTTPRequestPtr& http_request,  // NOLINT
                    TCPConnectionPtr& tcp_conn);
  void HandleJobSubmitURI(HTTPRequestPtr& http_request,  // NOLINT
                          TCPConnectionPtr& tcp_conn);
  void HandleJobsListURI(HTTPRequestPtr& http_request,  // NOLINT
                         TCPConnectionPtr& tcp_conn);
  void HandleJobStatusURI(HTTPRequestPtr& http_request,  // NOLINT
                          TCPConnectionPtr& tcp_conn);
  void HandleJobDTGURI(HTTPRequestPtr& http_request,  // NOLINT
                       TCPConnectionPtr& tcp_conn);
  void HandleRootURI(HTTPRequestPtr& http_request,  // NOLINT
                     TCPConnectionPtr& tcp_conn);
  void HandleResourcesListURI(HTTPRequestPtr& http_request,  // NOLINT
                              TCPConnectionPtr& tcp_conn);
  void HandleResourceURI(HTTPRequestPtr& http_request,  // NOLINT
                         TCPConnectionPtr& tcp_conn);
  void HandleInjectURI(HTTPRequestPtr& http_request,  // NOLINT
                       TCPConnectionPtr& tcp_conn);
  void HandleReferenceURI(HTTPRequestPtr& http_request,  // NOLINT
                          TCPConnectionPtr& tcp_conn);
  void HandleTaskURI(HTTPRequestPtr& http_request,  // NOLINT
                     TCPConnectionPtr& tcp_conn);
  void HandleShutdownURI(HTTPRequestPtr& http_request,  // NOLINT
                         TCPConnectionPtr& tcp_conn);
  void Shutdown(bool block);

  inline bool active() { return active_; }

 protected:
  typedef pair<const string, const string> ErrorMessage_t;
  void AddHeaderToTemplate(TemplateDictionary* dict, ResourceID_t uuid,
                           ErrorMessage_t* err);
  void AddFooterToTemplate(TemplateDictionary* dict);
  HTTPServerPtr coordinator_http_server_;
  shared_ptr<Coordinator> coordinator_;
  bool active_;
};

}  // namespace webui
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
