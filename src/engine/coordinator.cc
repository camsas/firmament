// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class implementation. This is subclassed by
// the platform-specific coordinator classes.

#include "engine/coordinator.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");

namespace firmament {

Coordinator::Coordinator(PlatformID platform_id)
  : platform_id_(platform_id) {
  // Start up a coordinator ccording to the platform parameter
  //platform_ = platform::GetByID(platform_id);
  string hostname = ""; //platform_.GetHostname();
  VLOG(1) << "Coordinator starting on host " << hostname << ", platform "
          << platform_id;
}

void Coordinator::Run() {
  // Coordinator starting -- set up and wait for workers to connect.
  while (!exit_) {  // main loop
    // Wait for events
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; hand-over to another coordinator if
  // possible?
}

}  // namespace firmament
