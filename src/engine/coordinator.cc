// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");
DEFINE_string(listen_uri, "tcp://localhost:9998",
              "The name/address/port to listen on.");

namespace firmament {

Coordinator::Coordinator(PlatformID platform_id)
  : platform_id_(platform_id) {
  // Start up a coordinator ccording to the platform parameter
  //platform_ = platform::GetByID(platform_id);
  string hostname = ""; //platform_.GetHostname();
  VLOG(1) << "Coordinator starting on host " << FLAGS_listen_uri
          << ", platform " << platform_id;

  switch (platform_id) {
    case PL_UNIX: {
      m_adapter_ = new StreamSocketsMessaging();
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }

}

void Coordinator::Run() {
  // Coordinator starting -- set up and wait for workers to connect.
  m_adapter_->Listen(FLAGS_listen_uri);
  while (!exit_) {  // main loop
    // Wait for events
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
}

void Coordinator::AwaitNextMessage() {
  VLOG_EVERY_N(2, 1) << "Waiting for next message...";
  ptime t(second_clock::local_time() + seconds(10));
  boost::thread::sleep(t);
}

}  // namespace firmament
