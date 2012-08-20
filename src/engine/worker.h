// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include <string>

// XXX(malte): Think about the Boost dependency!
#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#else
// Currently this won't build if __PLATFORM_HAS_BOOST__ is not defined.
#error __PLATFORM_HAS_BOOST__ not set, so cannot build worker!
#endif

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "messages/base_message.pb.h"
// XXX(malte): include order dependency
#include "platforms/unix/common.h"
#include "misc/messaging_interface.h"
#include "misc/protobuf_envelope.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets-inl.h"
#include "platforms/unix/stream_sockets_channel-inl.h"

namespace firmament {

using platform_unix::streamsockets::StreamSocketsMessaging;
using platform_unix::streamsockets::StreamSocketsChannel;

class Worker {
 public:
  explicit Worker(PlatformID platform_id);
  void Run();
  void AwaitNextMessage();
  bool RunCoordinatorDiscovery(const string &coordinator_uri);
  bool ConnectToCoordinator(const string& coordinator_uri);
  inline PlatformID platform_id() {
    return platform_id_;
  }

 protected:
  PlatformID platform_id_;
#ifdef __PLATFORM_HAS_BOOST__
  boost::shared_ptr<StreamSocketsMessaging> m_adapter_;
#else
  StreamSocketsMessaging *m_adapter_;
#endif
  StreamSocketsChannel<BaseMessage> chan_;
  bool exit_;
  string coordinator_uri_;
  ResourceDescriptor resource_desc_;
  ResourceID_t uuid_;

  ResourceID_t GenerateUUID();
  void HandleWrite(const boost::system::error_code& error,
                   size_t bytes_transferred);
};

}  // namespace firmament

#endif
