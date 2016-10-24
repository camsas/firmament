/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
                     const http::request_ptr& http_request,
                     const tcp::connection_ptr& tcp_conn);
  void FinishOkResponse(const http::response_writer_ptr& writer);
  void Init(uint16_t port);
  http::response_writer_ptr InitOkResponse(
      const http::request_ptr& http_request,
      const tcp::connection_ptr& tcp_conn);
  void LogRequest(const http::request_ptr& http_request);
  void HandleFaviconURI(const http::request_ptr& http_request,
                        const tcp::connection_ptr& tcp_conn);
  void HandleCollectlGraphsURI(const http::request_ptr& http_request,
                               const tcp::connection_ptr& tcp_conn);
  void HandleCollectlRawURI(const http::request_ptr& http_request,
                            const tcp::connection_ptr& tcp_conn);
  void HandleJobCompletionURI(const http::request_ptr& http_request,
                              const tcp::connection_ptr& tcp_conn);
  void HandleJobURI(const http::request_ptr& http_request,
                    const tcp::connection_ptr& tcp_conn);
  void HandleJobSubmitURI(const http::request_ptr& http_request,
                          const tcp::connection_ptr& tcp_conn);
  void HandleJobsListURI(const http::request_ptr& http_request,
                         const tcp::connection_ptr& tcp_conn);
  void HandleJobStatusURI(const http::request_ptr& http_request,
                          const tcp::connection_ptr& tcp_conn);
  void HandleJobDTGURI(const http::request_ptr& http_request,
                       const tcp::connection_ptr& tcp_conn);
  void HandleECDetailsURI(const http::request_ptr& http_request,
                          const tcp::connection_ptr& tcp_conn);
  void HandleLogURI(const http::request_ptr& http_request,
                    const tcp::connection_ptr& tcp_conn);
  void HandleRootURI(const http::request_ptr& http_request,
                     const tcp::connection_ptr& tcp_conn);
  void HandleResourcesListURI(const http::request_ptr& http_request,
                              const tcp::connection_ptr& tcp_conn);
  void HandleResourcesTopologyURI(const http::request_ptr& http_request,
                                  const tcp::connection_ptr& tcp_conn);
  void HandleResourceURI(const http::request_ptr& http_request,
                         const tcp::connection_ptr& tcp_conn);
  void HandleInjectURI(const http::request_ptr& http_request,
                       const tcp::connection_ptr& tcp_conn);
  void HandleReferenceURI(const http::request_ptr& http_request,
                          const tcp::connection_ptr& tcp_conn);
  void HandleReferencesListURI(const http::request_ptr& http_request,
                               const tcp::connection_ptr& tcp_conn);
  void HandleSchedURI(const http::request_ptr& http_request,
                      const tcp::connection_ptr& tcp_conn);
  void HandleSchedCostModelURI(const http::request_ptr& http_request,
                               const tcp::connection_ptr& tcp_conn);
  void HandleSchedFlowGraphURI(const http::request_ptr& http_request,
                               const tcp::connection_ptr& tcp_conn);
  void HandleStatisticsURI(const http::request_ptr& http_request,
                           const tcp::connection_ptr& tcp_conn);
  void HandleTasksListURI(const http::request_ptr& http_request,
                          const tcp::connection_ptr& tcp_conn);
  void HandleTaskURI(const http::request_ptr& http_request,
                     const tcp::connection_ptr& tcp_conn);
  void HandleTaskLogURI(const http::request_ptr& http_request,
                        const tcp::connection_ptr& tcp_conn);
  void HandleShutdownURI(const http::request_ptr& http_request,
                         const tcp::connection_ptr& tcp_conn);
  void RedirectResponse(const http::request_ptr& http_request,
                        const tcp::connection_ptr& tcp_conn,
                        const string& location);
  void ServeFile(const string& filename, const tcp::connection_ptr& tcp_conn,
                 const http::request_ptr& http_request,
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
  uint16_t port_;
};

}  // namespace webui
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_HTTP_UI_H
