// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Initialization code for coordinator binary. This delegates to the
// Coordinator class almost immediately after launching.

#include "base/common.h"
#include "engine/coordinator.h"
#include "platforms/common.h"

#include "platforms/common.pb.h"

using namespace firmament;  // NOLINT

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

  LOG(INFO) << "Firmament coordinator starting (Platform: " << platform_id
            << ") ...";
  boost::shared_ptr<Coordinator> coordinator(new Coordinator(platform_id));

  coordinator->Run();

  LOG(INFO) << "Coordinator's Run() method returned; terminating...";
}
