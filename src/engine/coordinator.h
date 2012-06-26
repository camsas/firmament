// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class definition. This is subclassed by the
// platform-specific coordinator classes.

#ifndef FIRMAMENT_ENGINE_COORDINATOR_H
#define FIRMAMENT_ENGINE_COORDINATOR_H

#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets.h"
#include "engine/coordinator_http_ui.h"

using namespace boost::posix_time;
using namespace boost::gregorian;

namespace firmament {

class Coordinator {
 public:
  Coordinator(PlatformID platform_id);
  void Run();
  void AwaitNextMessage();
  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
  bool exit_;
  string coordinator_uri_;
  MessagingInterface* m_adapter_;
  CoordinatorHTTPUI* c_http_ui_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
