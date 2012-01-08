// TODO: header

#include <stdint.h>
#include <iostream>

#include "base/common.h"
#include "engine/worker.h"
#include "platforms/common.h"

#include "platforms/common.pb.h"

using namespace firmament;

DECLARE_string(platform);

int main(int argc, char *argv[]) {
  VLOG(1) << "Calling common::InitFirmament";
  common::InitFirmament(argc, argv);

  // TODO(malte): support for automatic platform detection?
  // TODO(malte): validation of FLAGS_platform
  PlatformID platform_id = GetPlatformID(FLAGS_platform);

  LOG(INFO) << "Firmament worker starting (Platform: " << platform_id
            << ") ...";
  Worker worker(platform_id);
  worker.Test();
}
