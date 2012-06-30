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
#include <boost/uuid/uuid.hpp>

#include "base/common.h"
#include "base/types.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets.h"
#include "engine/coordinator_http_ui.h"
#include "engine/topology_manager.h"

using namespace boost::posix_time;
using namespace boost::gregorian;
using boost::scoped_ptr;
using firmament::machine::topology::TopologyManager;

namespace firmament {

// Forward declaration
class CoordinatorHTTPUI;

class Coordinator {
 public:
  explicit Coordinator(PlatformID platform_id);
  void Run();
  void AwaitNextMessage();
  inline PlatformID platform_id() {
    return platform_id_;
  }
  inline ResourceID_t get_uuid() { return uuid_; }
 protected:
  ResourceID_t GenerateUUID();

  PlatformID platform_id_;
  bool exit_;
  string coordinator_uri_;
  MessagingInterface* m_adapter_;
  ResourceID_t uuid_;
  scoped_ptr<CoordinatorHTTPUI> c_http_ui_;
  scoped_ptr<TopologyManager> topology_manager_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
