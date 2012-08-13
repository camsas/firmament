// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//

#include "engine/coordinator_http_ui.h"

#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>

namespace firmament {

CoordinatorHTTPUI::CoordinatorHTTPUI(Coordinator *coordinator) {
  coordinator_ = coordinator;
}

CoordinatorHTTPUI::~CoordinatorHTTPUI() {
  LOG(INFO) << "Coordinator HTTP UI server shut down.";
}

void CoordinatorHTTPUI::handleRootURI(HTTPRequestPtr& http_request,
                                      TCPConnectionPtr& tcp_conn) {
  VLOG(2) << "[HTTPREQ] Serving " << http_request->getResource();
  static const std::string kHTMLStart("<html><body>\n");
  static const std::string kHTMLEnd("</body></html>\n");

  HTTPResponseWriterPtr
    writer(HTTPResponseWriter::create(tcp_conn,
                                      *http_request,
                                      boost::bind(&TCPConnection::finish,
                                                  tcp_conn)));
  HTTPResponse& r = writer->getResponse();
  r.setStatusCode(HTTPTypes::RESPONSE_CODE_OK);
  r.setStatusMessage(HTTPTypes::RESPONSE_MESSAGE_OK);

  HTTPTypes::QueryParams& params = http_request->getQueryParams();

  writer->writeNoCopy(kHTMLStart);
  writer->write(coordinator_->uuid());
  //writer->write(http_request->getResource());

/*  if (params.size() > 0) {
    writer->write(" has the following parameters: <br>");
    for (HTTPTypes::QueryParams::const_iterator i = params.begin();
         i != params.end(); ++i) {
      writer->write(i->first);
      writer->write("=");
      writer->write(i->second);
      writer->write("<br>");
    }
  } else {
    writer->write(" has no parameter.");
  }*/
  writer->writeNoCopy(kHTMLEnd);
  writer->send();
}

void CoordinatorHTTPUI::init(uint32_t port) {
  try {
    // Fail if we are not assured that no existing server object is stored.
    if (coordinator_http_server_ != NULL) {
      LOG(FATAL) << "Trying to initialized an HTTP server that has already "
                 << "been initialized!";
    }
    // Otherwise, make such an object and store it.
    coordinator_http_server_.reset(new HTTPServer(port));
    coordinator_http_server_->addResource("/", boost::bind(&CoordinatorHTTPUI::handleRootURI, this, _1, _2));
    coordinator_http_server_->start();  // spawns a thread!
    LOG(INFO) << "Coordinator HTTP interface up!";
  } catch (std::exception& e) {
    LOG(ERROR) << "Failed running the coordinator's HTTP UI due to " <<  e.what();
  }
}

}  // namespace firmament
