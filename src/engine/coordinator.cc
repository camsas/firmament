// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");
DEFINE_string(listen_addr, "localhost", "The name/address to listen on.");
DEFINE_int32(port, 9998, "Port to listen on (networked transports only).");

namespace firmament {

Coordinator::Coordinator(PlatformID platform_id)
  : platform_id_(platform_id) {
  // Start up a coordinator ccording to the platform parameter
  //platform_ = platform::GetByID(platform_id);
  string hostname = ""; //platform_.GetHostname();
  VLOG(1) << "Coordinator starting on host " << FLAGS_listen_addr
          << ", platform " << platform_id;

  switch (platform_id) {
    case UNIX: {
      m_adapter_ = new StreamSocketsMessaging();
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }

}

void Coordinator::Run() {
  // Coordinator starting -- set up and wait for workers to connect.
  m_adapter_->Listen(FLAGS_listen_addr, FLAGS_port);
  while (!exit_) {  // main loop
    // Wait for events
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
}

}  // namespace firmament
