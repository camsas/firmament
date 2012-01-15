// TODO:header

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
