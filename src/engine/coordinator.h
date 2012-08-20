// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class definition. This is subclassed by the
// platform-specific coordinator classes.

#ifndef FIRMAMENT_ENGINE_COORDINATOR_H
#define FIRMAMENT_ENGINE_COORDINATOR_H

#include <string>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/scoped_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/uuid/uuid.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build coordinator!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
// XXX(malte): include order dependency
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets.h"
#include "engine/coordinator_http_ui.h"
#include "engine/topology_manager.h"

namespace firmament {

#ifdef __PLATFORM_HAS_BOOST__
using boost::scoped_ptr;
using boost::shared_ptr;
#endif
using machine::topology::TopologyManager;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsMessaging;

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
  inline ResourceID_t uuid() { return uuid_; }

 protected:
  ResourceID_t GenerateUUID();
  void HandleRecv(const boost::system::error_code& error,
                  size_t bytes_transferred,
                  Envelope<BaseMessage>* env);

  PlatformID platform_id_;
  bool exit_;
  string coordinator_uri_;
  shared_ptr<StreamSocketsMessaging> m_adapter_;
  ResourceID_t uuid_;
  scoped_ptr<CoordinatorHTTPUI> c_http_ui_;
  scoped_ptr<TopologyManager> topology_manager_;
  ResourceDescriptor resource_desc_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
