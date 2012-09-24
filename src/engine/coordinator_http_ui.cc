// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "engine/coordinator_http_ui.h"

#include <string>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>
#include <google/protobuf/text_format.h>

#include "base/job_desc.pb.h"
#include "engine/coordinator.h"

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
  // Kill the server without waiting for connections to terminate
  if (coordinator_http_server_->isListening()) {
    coordinator_http_server_->stop(false);
    coordinator_http_server_->join();
    LOG(INFO) << "Coordinator HTTP UI server stopped.";
  }
  LOG(INFO) << "Coordinator HTTP UI server destroyed.";
}

void CoordinatorHTTPUI::HandleJobSubmitURI(HTTPRequestPtr& http_request,  // NOLINT
                                           TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  // Check if we have a JobDescriptor as part of the POST parameters
  HTTPTypes::QueryParams& params = http_request->getQueryParams();
  string* job_descriptor_param = FindOrNull(params, "test");
  if (http_request->getMethod() != "POST" || !job_descriptor_param) {
    ErrorResponse(HTTPTypes::RESPONSE_CODE_SERVER_ERROR, http_request,
                  tcp_conn);
    return;
  }
  // We're okay to continue
  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Submit the JD to the coordinator
  JobDescriptor job_descriptor;
  google::protobuf::TextFormat::ParseFromString(*job_descriptor_param,
                                                &job_descriptor);
  VLOG(1) << "JD:" << job_descriptor.DebugString();
  string job_id = coordinator_->SubmitJob(job_descriptor);
  // Return the job ID to the client
  writer->write(job_id);

  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleRootURI(HTTPRequestPtr& http_request,  // NOLINT
                                      TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Individual to this request
  //HTTPTypes::QueryParams& params = http_request->getQueryParams();
  writer->write("<h1>");
  writer->write(coordinator_->uuid());
  writer->write("</h1>");
  writer->write("<a href=\"/resources\">Resources</a><br />");
  writer->write("<a href=\"/jobs\">Jobs</a><br />");
  writer->write("<a href=\"/shutdown\">Shutdown</a>");

  FinishOkResponse(writer);
}

void CoordinatorHTTPUI::HandleResourcesURI(HTTPRequestPtr& http_request,  // NOLINT
                                           TCPConnectionPtr& tcp_conn) {  // NOLINT
  LogRequest(http_request);

  HTTPResponseWriterPtr writer = InitOkResponse(http_request, tcp_conn);

  // Get resource information from coordinator
  vector<ResourceDescriptor> resources = coordinator_->associated_resources();
  uint64_t i = 0;
  writer->write("<h1>");
  writer->write(coordinator_->uuid());
  writer->write("</h1>");
  writer->write("<table border=\"1\"><tr><th></th><th>Resource ID</th><th>Friendly name</th><th>State</th></tr>");
  for (vector<ResourceDescriptor>::const_iterator rd_iter =
       resources.begin();
       rd_iter != resources.end();
       ++rd_iter) {
    writer->write("<tr><td>");
    writer->write(i);
    writer->write("</td><td>");
    writer->write(rd_iter->uuid());
    writer->write("</td><td>");
    writer->write(rd_iter->friendly_name());
    writer->write("</td><td>");
    writer->write(rd_iter->state());
    writer->write("</td></tr>");
    ++i;
  }
  writer->write("</table>");

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

void CoordinatorHTTPUI::ErrorResponse(
    const unsigned int error_code,
    HTTPRequestPtr http_request,
    TCPConnectionPtr tcp_conn) {
  HTTPResponseWriterPtr writer = HTTPResponseWriter::create(
      tcp_conn, *http_request, boost::bind(&TCPConnection::finish,
                                           tcp_conn));
  HTTPResponse& r = writer->getResponse();
  r.setStatusCode(error_code);
  //r.setStatusMessage("test");
  writer->send();
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
    // Job submission
    coordinator_http_server_->addResource("/job/submit/", boost::bind(
        &CoordinatorHTTPUI::HandleJobSubmitURI, this, _1, _2));
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

void CoordinatorHTTPUI::Shutdown(bool block) {
  LOG(INFO) << "Coordinator HTTP UI server shutting down on request.";
  coordinator_http_server_->stop(block);
}

}  // namespace webui
}  // namespace firmament
