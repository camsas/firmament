// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

using boost::posix_time::ptime;
using boost::posix_time::second_clock;
using boost::posix_time::seconds;

namespace firmament {

using platform_unix::streamsockets::StreamSocketsMessaging;
using platform_unix::streamsockets::StreamSocketsChannel;

class Worker {
 public:
  Worker(PlatformID platform_id);
  void Run();
  void AwaitNextMessage();
  bool RunCoordinatorDiscovery(const string &coordinator_uri);
  bool ConnectToCoordinator(const string& coordinator_uri);
  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
  boost::shared_ptr<StreamSocketsMessaging> m_adapter_;
  StreamSocketsChannel<Message> chan_;
  bool exit_;
  string coordinator_uri_;
  ResourceDescriptor resource_desc_;
  ResourceID_t uuid_;

  ResourceID_t GenerateUUID();
};

}  // namespace firmament

#endif
