// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent coordinator class definition. This is subclassed by the
// platform-specific coordinator classes.

#ifndef FIRMAMENT_ENGINE_COORDINATOR_H
#define FIRMAMENT_ENGINE_COORDINATOR_H

#include <string>
#include <map>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/uuid/uuid.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build coordinator!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/job_desc.pb.h"
#include "base/resource_desc.pb.h"
// XXX(malte): include order dependency
#include "platforms/unix/common.h"
#include "messages/heartbeat_message.pb.h"
#include "messages/registration_message.pb.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/signal_handler.h"
#include "platforms/unix/stream_sockets_adapter.h"
#include "platforms/unix/stream_sockets_adapter-inl.h"
#ifdef __HTTP_UI__
#include "engine/coordinator_http_ui.h"
#endif
#include "engine/topology_manager.h"

namespace firmament {

//using __gnu_cxx::hash_map;

using machine::topology::TopologyManager;
using platform_unix::SignalHandler;
using platform_unix::streamsockets::StreamSocketsChannel;
using platform_unix::streamsockets::StreamSocketsAdapter;

#ifdef __HTTP_UI__
// Forward declaration
namespace webui {
class CoordinatorHTTPUI;
}  // namespace webui
using webui::CoordinatorHTTPUI;
#endif

class Coordinator : public boost::enable_shared_from_this<Coordinator> {
 public:
  explicit Coordinator(PlatformID platform_id);
  virtual ~Coordinator();
  void Run();
  void AwaitNextMessage();
  void Shutdown(const string& reason);
  const string SubmitJob(const JobDescriptor& job_descriptor);

  inline PlatformID platform_id() {
    return platform_id_;
  }
  inline ResourceID_t uuid() { return uuid_; }
  vector<ResourceDescriptor> associated_resources() {
    vector<ResourceDescriptor> ref_vec;
    for (ResourceMap_t::const_iterator res_iter =
         associated_resources_.begin();
         res_iter != associated_resources_.end();
         ++res_iter) {
      ref_vec.push_back(res_iter->second.first);
    }
    return ref_vec;
  };

 protected:
  ResourceID_t GenerateUUID();
  void HandleIncomingMessage(BaseMessage *bm);
  void HandleHeartbeat(const HeartbeatMessage& msg);
  void HandleRegistrationRequest(const RegistrationMessage& msg);
  void HandleRecv(const boost::system::error_code& error,
                  size_t bytes_transferred,
                  Envelope<BaseMessage>* env);
#if (BOOST_VERSION < 104700)
  // compatible with C-style signal handler setup
  static void HandleSignal(int signum);
#else
  // Boost ASIO signal handler setup
  void HandleSignal(int signum);
#endif

  PlatformID platform_id_;
  static bool exit_;
  string coordinator_uri_;
  shared_ptr<StreamSocketsAdapter<BaseMessage> > m_adapter_;
#ifdef __HTTP_UI__
  scoped_ptr<CoordinatorHTTPUI> c_http_ui_;
#endif
  scoped_ptr<TopologyManager> topology_manager_;
  // A map of resources associated with this coordinator.
  // The first component of the pair is the resource descriptor, the second is
  // the timestamp when the latest heartbeat or message was received from this
  // resource..
  ResourceMap_t associated_resources_;
  // This coordinator's own resource descriptor.
  ResourceDescriptor resource_desc_;
  ResourceID_t uuid_;
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
