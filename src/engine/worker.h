// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "platforms/common.h"
#include "platforms/unix/messaging_streamsockets.h"
#include "platforms/unix/messaging_streamsockets-inl.h"

using boost::posix_time::ptime;
using boost::posix_time::second_clock;
using boost::posix_time::seconds;

namespace firmament {

class Worker {
 public:
  Worker(PlatformID platform_id);
  void Run();
  void AwaitNextMessage() {
    TestMessage tm;
    VLOG_EVERY_N(2, 1) << "Receiving (sync)...";
    chan_.RecvS(&tm);
    VLOG_EVERY_N(2, 1) << "Waiting for next message...";
    ptime t(second_clock::local_time() + seconds(10));
    boost::thread::sleep(t);
  };
  bool RunCoordinatorDiscovery(const string &coordinator_uri) {
    LOG(FATAL) << "Coordinator auto-discovery is not implemented yet. "
               << "coordinator_uri given was: " << coordinator_uri;
    return false;
  }
  bool ConnectToCoordinator(const string& coordinator_uri) {
    m_adapter_.EstablishChannel(coordinator_uri, &chan_);
    // Send registration message
    return true;
  }

  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
  platform_unix::streamsockets::StreamSocketsMessaging m_adapter_;
  platform_unix::streamsockets::StreamSocketsChannel<TestMessage> chan_;
  bool exit_;
  string coordinator_uri_;
};

}  // namespace firmament

#endif
