// TODO:header

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include "base/common.h"

namespace firmament {

class Worker {
 public:
  Worker(PlatformID platform_id);
  int64_t Test();
  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
};

}  // namespace firmament

#endif
