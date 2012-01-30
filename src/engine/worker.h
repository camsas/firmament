// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include "base/common.h"
#include "platforms/common.h"

namespace firmament {

class Worker {
 public:
  Worker(PlatformID platform_id);
  void Run();
  void AwaitNextMessage() {};
  bool RunCoordinatorDiscovery(string *coordinator_uri) {
    VLOG(1) << coordinator_uri;
    return true;
  }

  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
  bool exit_;
  string coordinator_uri_;
};

}  // namespace firmament

#endif
