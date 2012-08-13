// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Initialization code for worker binary. This delegates to the Worker class
// almost immediately after launching.

#include <stdint.h>
#include <iostream>

#include "base/common.h"
#include "engine/worker.h"
#include "platforms/common.h"

#include "platforms/common.pb.h"

using namespace firmament;

// --platform argument: string matching members of the <PlatformID> enum
DECLARE_string(platform);

// The main method: initializes, parses arguments and sets up a worker for
// the platform we're running on.
int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  // TODO(malte): support for automatic platform detection?
  // TODO(malte): validation of FLAGS_platform
  PlatformID platform_id = GetPlatformID(FLAGS_platform);

  LOG(INFO) << "Firmament worker starting (Platform: " << platform_id
            << ") ...";
  Worker worker(platform_id);

  worker.Run();

  LOG(INFO) << "Worker's Run() method returned; terminating...";
}
