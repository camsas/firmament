// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class implementation. This is subclassed by the
// platform-specific worker classes.

#include <iostream>

#include "engine/worker.h"
//#include "platforms/common.h"

DEFINE_string(platform, "asdf", "unix");

namespace firmament {

Worker::Worker(PlatformID platform_id)
  : platform_id_(platform_id) {
  // Start up a worker according to the platform parameter
  //platform_ = platform::GetByID(platform_id);
  string hostname = ""; //platform_.GetHostname();
  VLOG(1) << "Worker starting on host " << hostname << ", platform "
          << platform_id;
}

int64_t Worker::Test() {
  std::cout << "test" << std::endl;
  return 1;
}

}  // namespace firmament
