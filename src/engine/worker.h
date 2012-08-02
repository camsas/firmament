// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class definition. This is subclassed by the
// platform-specific worker classes.

#ifndef FIRMAMENT_ENGINE_WORKER_H
#define FIRMAMENT_ENGINE_WORKER_H

#include "base/common.h"
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
  void AwaitNextMessage() {
    TestMessage tm;
    tm.set_test(444);
    Envelope<Message> envelope(&tm);
    VLOG_EVERY_N(2, 1) << "Sending (sync)...";
    chan_.SendS(envelope);
    VLOG_EVERY_N(2, 1) << "Waiting for next message...";
    boost::this_thread::sleep(seconds(10));
  };
  bool RunCoordinatorDiscovery(const string &coordinator_uri) {
    LOG(FATAL) << "Coordinator auto-discovery is not implemented yet. "
               << "coordinator_uri given was: " << coordinator_uri;
    return false;
  }
  bool ConnectToCoordinator(const string& coordinator_uri) {
    return m_adapter_->EstablishChannel(coordinator_uri, &chan_);
    // TODO(malte): Send registration message
  }

  inline PlatformID platform_id() {
    return platform_id_;
  }
 protected:
  PlatformID platform_id_;
  boost::shared_ptr<StreamSocketsMessaging> m_adapter_;
  StreamSocketsChannel<Message> chan_;
  bool exit_;
  string coordinator_uri_;
};

}  // namespace firmament

#endif
