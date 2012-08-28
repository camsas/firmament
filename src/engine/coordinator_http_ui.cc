// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "engine/coordinator_http_ui.h"
#include "engine/coordinator.h"

#include <string>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>

namespace firmament {
namespace webui {

using pion::net::HTTPResponseWriter;
using pion::net::HTTPResponseWriterPtr;
using pion::net::HTTPResponse;
using pion::net::HTTPTypes;
using pion::net::HTTPServer;
using pion::net::TCPConnection;

CoordinatorHTTPUI::CoordinatorHTTPUI(shared_ptr<Coordinator> coordinator) {
  coordinator_ = coordinator;
}

CoordinatorHTTPUI::~CoordinatorHTTPUI() {
  LOG(INFO) << "Coordinator HTTP UI server shut down.";
}

void CoordinatorHTTPUI::HandleRootURI(HTTPRequestPtr& http_request,  // NOLINT
                                      TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Individual to this request
  HTTPTypes::QueryParams& params = http_request->getQueryParams();
  writer->write(coordinator_->uuid());

  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesURI(HTTPRequestPtr& http_request,  // NOLINT
                                           TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Individual to this request
  writer->write("test");

  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleInjectURI(HTTPRequestPtr& http_request,  // NOLINT
                                        TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Individual to this request
  if (http_request->getMethod() != "POST") {
    // return an error
    writer->write("POST a message to this URL to inject it.");
  } else {
    writer->write("ok");
  }

  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleShutdownURI(HTTPRequestPtr& http_request,  // NOLINT
                                          TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Individual to this request
  string reason = "HTTP request from " + tcp_conn->getRemoteIp().to_string();
  coordinator_->Shutdown(reason);
  writer->write("Shutdown for coordinator initiated.");

  FinishOkResponse(writer);
}

HTTPResponseWriterPtr CoordinatorHTTPUI::InitOkResponse(
    HTTPRequestPtr http_request,
    TCPConnectionPtr tcp_conn) {
  HTTPResponseWriterPtr writer = HTTPResponseWriter::create(
      tcp_conn, *http_request, boost::bind(&TCPConnection::finish,
                                           tcp_conn));
  HTTPResponse& r = writer->getResponse();
  r.setStatusCode(HTTPTypes::RESPONSE_CODE_OK);
  r.setStatusMessage(HTTPTypes::RESPONSE_MESSAGE_OK);
  // Header
  writer->writeNoCopy(kHTMLStart);
  return writer;
}

void CoordinatorHTTPUI::FinishOkResponse(HTTPResponseWriterPtr writer) {
  writer->writeNoCopy(kHTMLEnd);
  writer->send();
}

void CoordinatorHTTPUI::LogRequest(HTTPRequestPtr& http_request) {
  LOG(INFO) << "[HTTPREQ] Serving " << http_request->getResource();
}

void CoordinatorHTTPUI::Init(uint16_t port) {
  try {
    // Fail if we are not assured that no existing server object is stored.
    if (coordinator_http_server_ != NULL) {
      LOG(FATAL) << "Trying to initialized an HTTP server that has already "
                 << "been initialized!";
    }
    // Otherwise, make such an object and store it.
    coordinator_http_server_.reset(new HTTPServer(port));
    // Bind handlers for different kinds of entry points
    // Root URI
    coordinator_http_server_->addResource("/", boost::bind(
        &CoordinatorHTTPUI::HandleRootURI, this, _1, _2));
    // Resource page
    coordinator_http_server_->addResource("/resources/", boost::bind(
        &CoordinatorHTTPUI::HandleResourcesURI, this, _1, _2));
    // Message injection
    coordinator_http_server_->addResource("/inject/", boost::bind(
        &CoordinatorHTTPUI::HandleInjectURI, this, _1, _2));
    // Shutdown request
    coordinator_http_server_->addResource("/shutdown/", boost::bind(
        &CoordinatorHTTPUI::HandleShutdownURI, this, _1, _2));
    // Start the HTTP server
    coordinator_http_server_->start();  // spawns a thread!
    LOG(INFO) << "Coordinator HTTP interface up!";
  } catch(const std::exception& e) {
    LOG(ERROR) << "Failed running the coordinator's HTTP UI due to "
               << e.what();
  }
}

}  // namespace webui
}  // namespace firmament
