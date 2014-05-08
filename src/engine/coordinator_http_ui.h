// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
#define FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H

#include <string>
#include <utility>

#include <pion/http/server.hpp>
#include <pion/http/types.hpp>
#include <pion/http/request.hpp>
#include <pion/http/response_writer.hpp>
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

using namespace pion;  // NOLINT
using ctemplate::TemplateDictionary;

class CoordinatorHTTPUI {
 public:
  explicit CoordinatorHTTPUI(shared_ptr<Coordinator> coordinator);
  virtual ~CoordinatorHTTPUI();
  void ErrorResponse(const unsigned int error_code,
                     http::request_ptr http_request,
                     tcp::connection_ptr tcp_conn);
  void FinishOkResponse(http::response_writer_ptr writer);
  void Init(uint16_t port);
  http::response_writer_ptr InitOkResponse(http::request_ptr http_request,
                                       tcp::connection_ptr tcp_conn);
  void LogRequest(const http::request_ptr& http_request);
  void HandleFaviconURI(http::request_ptr& http_request,  // NOLINT
                        tcp::connection_ptr& tcp_conn);
  void HandleJobURI(http::request_ptr& http_request,  // NOLINT
                    tcp::connection_ptr& tcp_conn);
  void HandleJobSubmitURI(http::request_ptr& http_request,  // NOLINT
                          tcp::connection_ptr& tcp_conn);
  void HandleJobsListURI(http::request_ptr& http_request,  // NOLINT
                         tcp::connection_ptr& tcp_conn);
  void HandleJobStatusURI(http::request_ptr& http_request,  // NOLINT
                          tcp::connection_ptr& tcp_conn);
  void HandleJobDTGURI(http::request_ptr& http_request,  // NOLINT
                       tcp::connection_ptr& tcp_conn);
  void HandleRootURI(http::request_ptr& http_request,  // NOLINT
                     tcp::connection_ptr& tcp_conn);
  void HandleResourcesListURI(http::request_ptr& http_request,  // NOLINT
                              tcp::connection_ptr& tcp_conn);
  void HandleResourcesTopologyURI(http::request_ptr& http_request,  // NOLINT
                                  tcp::connection_ptr& tcp_conn);
  void HandleResourceURI(http::request_ptr& http_request,  // NOLINT
                         tcp::connection_ptr& tcp_conn);
  void HandleInjectURI(http::request_ptr& http_request,  // NOLINT
                       tcp::connection_ptr& tcp_conn);
  void HandleReferenceURI(http::request_ptr& http_request,  // NOLINT
                          tcp::connection_ptr& tcp_conn);
  void HandleReferencesListURI(http::request_ptr& http_request,  // NOLINT
                               tcp::connection_ptr& tcp_conn);
  void HandleStatisticsURI(http::request_ptr& http_request,  // NOLINT
                           tcp::connection_ptr& tcp_conn);
  void HandleTaskURI(http::request_ptr& http_request,  // NOLINT
                     tcp::connection_ptr& tcp_conn);
  void HandleShutdownURI(http::request_ptr& http_request,  // NOLINT
                         tcp::connection_ptr& tcp_conn);
  void Shutdown(bool block);

  inline bool active() { return active_; }

 protected:
  typedef pair<const string, const string> ErrorMessage_t;
  void AddHeaderToTemplate(TemplateDictionary* dict, ResourceID_t uuid,
                           ErrorMessage_t* err);
  void AddFooterToTemplate(TemplateDictionary* dict);
  http::server_ptr coordinator_http_server_;
  shared_ptr<Coordinator> coordinator_;
  bool active_;
};

}  // namespace webui
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
