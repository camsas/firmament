// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

#include <boost/uuid/uuid_generators.hpp>
#include <string>

#include "misc/protobuf_envelope.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");
DEFINE_string(listen_uri, "tcp://localhost:9998",
              "The name/address/port to listen on.");
DEFINE_int32(http_ui_port, 8080,
             "The port that the HTTP UI will be served on; -1 to disable.");

namespace firmament {

using boost::shared_ptr;  // XXX(malte): think about this dependency

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
      m_adapter_.reset(new platform_unix::streamsockets::StreamSocketsMessaging());
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }

  // Start up HTTP interface
  if (FLAGS_http_ui_port > 0) {
    c_http_ui_.reset(new CoordinatorHTTPUI(this));
    c_http_ui_->init(FLAGS_http_ui_port);
  }

  // test topology detection
  topology_manager_->DebugPrintRawTopology();
}

void Coordinator::Run() {
  // Coordinator starting -- set up and wait for workers to connect.
  m_adapter_->Listen(FLAGS_listen_uri);
  while (!exit_) {  // main loop
    // Wait for events (i.e. messages from workers)
    // TODO(malte): we need to think about any actions that the coordinator
    // itself might need to take, and how they can be triggered
    VLOG(3) << "Hello from main loop!";
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
}

void Coordinator::AwaitNextMessage() {
  //VLOG_EVERY_N(2, 1000) << "Waiting for next message...";
  uint64_t i = 0;
  for (vector<shared_ptr<StreamSocketsChannel<Message> > >::const_iterator chan_iter =
       m_adapter_->active_channels().begin();
       chan_iter < m_adapter_->active_channels().end();
       ++chan_iter) {
    VLOG(1) << "Trying to receive on channel " << i;
    TestMessage tm;
    Envelope<google::protobuf::Message> envelope(&tm);
    (*chan_iter)->RecvS(&envelope);
    VLOG(1) << "Received message of size " << tm.ByteSize()
            << ", type " << tm.GetTypeName()
            << ", contents: " << tm.DebugString();
    ++i;
  }
  VLOG(2) << "Looped over " << i << " channels.";
  //boost::thread::yield();
  boost::this_thread::sleep(boost::posix_time::seconds(1));
}

ResourceID_t Coordinator::GenerateUUID() {
  boost::uuids::random_generator gen;
  return gen();
}

}  // namespace firmament
