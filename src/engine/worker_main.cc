// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Initialization code for worker binary. This delegates to the Worker class
// almost immediately after launching.

#include "base/common.h"
#include "engine/worker.h"
#include "platforms/common.h"

using namespace firmament;  // NOLINT

// The main method: initializes, parses arguments and sets up a worker for
// the platform we're running on.
int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  LOG(INFO) << "Firmament worker starting ...";
  Worker worker();

  worker.Run();

  LOG(INFO) << "Worker's Run() method returned; terminating...";
}
