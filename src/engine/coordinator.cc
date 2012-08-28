// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

#include <string>
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#endif

#include "base/resource_desc.pb.h"
#include "messages/base_message.pb.h"
#include "messages/heartbeat_message.pb.h"
#include "misc/protobuf_envelope.h"
#include "misc/map-util.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");
DEFINE_string(listen_uri, "tcp://localhost:9998",
              "The name/address/port to listen on.");
#ifdef __HTTP_UI__
DEFINE_int32(http_ui_port, 8080,
             "The port that the HTTP UI will be served on; -1 to disable.");
#endif

namespace firmament {

Coordinator::Coordinator(PlatformID platform_id)
  : platform_id_(platform_id),
    uuid_(GenerateUUID()),
    topology_manager_(new TopologyManager()),
    exit_(false) {
  // Start up a coordinator ccording to the platform parameter
  // platform_ = platform::GetByID(platform_id);
  string hostname = "";  // platform_.GetHostname();
  VLOG(1) << "Coordinator starting on host " << FLAGS_listen_uri
          << ", platform " << platform_id;

  switch (platform_id) {
    case PL_UNIX: {
      m_adapter_.reset(
          new platform_unix::streamsockets::
          StreamSocketsAdapter<BaseMessage>());
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }

#ifdef __HTTP_UI__
  // Start up HTTP interface
  if (FLAGS_http_ui_port > 0) {
    c_http_ui_.reset(new CoordinatorHTTPUI(this));
    c_http_ui_->init(FLAGS_http_ui_port);
  }
#endif

  // test topology detection
  topology_manager_->DebugPrintRawTopology();
}

void Coordinator::Run() {
  // Coordinator starting -- set up and wait for workers to connect.
  m_adapter_->Listen(FLAGS_listen_uri);
  m_adapter_->RegisterAsyncMessageReceiptCallback(
      boost::bind(&Coordinator::HandleIncomingMessage, this, _1));
  while (!exit_) {  // main loop
    // Wait for events (i.e. messages from workers)
    // TODO(malte): we need to think about any actions that the coordinator
    // itself might need to take, and how they can be triggered
    VLOG(2) << "Hello from main loop!";
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
}

void Coordinator::AwaitNextMessage() {
  VLOG(2) << "Waiting for next message from adapter...";
  m_adapter_->AwaitNextMessage();
  boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void Coordinator::HandleRecv(const boost::system::error_code& error,
                             size_t bytes_transferred,
                             Envelope<BaseMessage>* env) {
  if (error) {
    LOG(WARNING) << "Asynchronous receive call returned an error: "
                 << error.message();
    return;
  }
  VLOG(2) << "Received " << bytes_transferred << " bytes asynchronously, "
          << "in envelope at " << env << ", representing message " << *env;
  BaseMessage *bm = env->data();
  HandleIncomingMessage(bm);
  delete env;
}

void Coordinator::HandleIncomingMessage(BaseMessage *bm) {
  // Heartbeat message
  if (bm->HasExtension(heartbeat_extn)) {
    const HeartbeatMessage& msg = bm->GetExtension(heartbeat_extn);
    HandleHeartbeat(msg);
  }
}

void Coordinator::HandleHeartbeat(const HeartbeatMessage& msg) {
  boost::uuids::string_generator gen;
  boost::uuids::uuid uuid = gen(msg.uuid());
  ResourceDescriptor* rd = FindOrNull(associated_resources_, uuid);
  if (!rd) {
    LOG(INFO) << "NEW RESOURCE (uuid: " << msg.uuid() << ")";
    // N.B.: below will copy the resource descriptor
    CHECK(InsertIfNotPresent(&associated_resources_, uuid,
                             msg.res_desc()));
  } else {
    LOG(INFO) << "HEARTBEAT from resource " << msg.uuid();
  }
}

ResourceID_t Coordinator::GenerateUUID() {
  boost::uuids::random_generator gen;
  return gen();
}

}  // namespace firmament
