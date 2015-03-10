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
                        tcp::connection_ptr& tcp_conn); // NOLINT
  void HandleJobURI(http::request_ptr& http_request, // NOLINT
                    tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleJobSubmitURI(http::request_ptr& http_request, // NOLINT
                          tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleJobsListURI(http::request_ptr& http_request, // NOLINT
                         tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleJobStatusURI(http::request_ptr& http_request, // NOLINT
                          tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleJobDTGURI(http::request_ptr& http_request, // NOLINT
                       tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleRootURI(http::request_ptr& http_request, // NOLINT
                     tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleResourcesListURI(http::request_ptr& http_request, // NOLINT
                              tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleResourcesTopologyURI(http::request_ptr& http_request, // NOLINT
                                  tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleResourceURI(http::request_ptr& http_request, // NOLINT
                         tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleInjectURI(http::request_ptr& http_request, // NOLINT
                       tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleReferenceURI(http::request_ptr& http_request, // NOLINT
                          tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleReferencesListURI(http::request_ptr& http_request, // NOLINT
                               tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleSchedURI(http::request_ptr& http_request, // NOLINT
                      tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleStatisticsURI(http::request_ptr& http_request, // NOLINT
                           tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleTaskURI(http::request_ptr& http_request, // NOLINT
                     tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleTaskLogURI(http::request_ptr& http_request, // NOLINT
                        tcp::connection_ptr& tcp_conn);  // NOLINT
  void HandleShutdownURI(http::request_ptr& http_request, // NOLINT
                         tcp::connection_ptr& tcp_conn);  // NOLINT
  void ServeFile(const string& filename, tcp::connection_ptr& tcp_conn,
                 http::request_ptr& http_request,
                 http::response_writer_ptr writer);
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
