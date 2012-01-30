// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class implementation. This is subclassed by the
// platform-specific worker classes.

#include "engine/worker.h"

DEFINE_string(platform, "AUTO", "The platform we are running on, or AUTO for "
              "attempting automatic discovery.");

namespace firmament {

Worker::Worker(PlatformID platform_id)
  : platform_id_(platform_id) {
  // Start up a worker according to the platform parameter
  //platform_ = platform::GetByID(platform_id);
  string hostname = ""; //platform_.GetHostname();
  VLOG(1) << "Worker starting on host " << hostname << ", platform "
          << platform_id;
}

void Worker::Run() {
  // Worker starting -- first need to find a coordinator and connect to it.
  if (coordinator_uri_.empty()) {
    if (!RunCoordinatorDiscovery(&coordinator_uri_)) {
      LOG(FATAL) << "No coordinator URI set, and automatic coordinator "
                 << "discovery failed! Exiting...";
    }
  }

  while (!exit_) {  // main loop
    // Wait for events
    AwaitNextMessage();
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; terminate running
  // tasks etc.
}

}  // namespace firmament
