// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Platform-independent worker class implementation. This is subclassed by the
// platform-specific worker classes.

#include "engine/worker.h"

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#include <boost/enable_shared_from_this.hpp>
#endif

#include "base/common.h"
#include "messages/heartbeat_message.pb.h"
#include "messages/registration_message.pb.h"
#include "platforms/common.pb.h"
#include "platforms/unix/stream_sockets_adapter.h"

// N.B.: We will be inheriting a bunch of standard flags from Node here (in
// addition to those specified below).
DEFINE_string(coordinator_uri, "", "The URI to contact the coordinator at.");
DEFINE_string(name, "", "A friendly name for this worker.");

namespace firmament {

#ifdef __PLATFORM_HAS_BOOST__
using boost::posix_time::ptime;
using boost::posix_time::second_clock;
using boost::posix_time::seconds;
#endif

Worker::Worker(PlatformID platform_id)
  : Node(platform_id, GenerateUUID()),
    chan_(new StreamSocketsChannel<BaseMessage>(
          StreamSocketsChannel<BaseMessage>::SS_TCP)),
    coordinator_uri_(FLAGS_coordinator_uri) {
  string hostname = "";  // platform_.GetHostname();
  VLOG(1) << "Worker starting on host " << hostname << ", platform "
          << platform_id;
  // Start up a worker according to the platform parameter
  switch (platform_id) {
    case PL_UNIX: {
      // Initiate UNIX worker.
      // worker_ = new UnixWorker();
      break;
    }
    default:
      LOG(FATAL) << "Unimplemented!";
  }

  // TODO(malte): fix this!
  resource_desc_.set_uuid(boost::uuids::to_string(uuid_));
  resource_desc_.set_schedulable(true);
  resource_desc_.set_state(ResourceDescriptor::RESOURCE_IDLE);

  if (!FLAGS_name.empty())
    resource_desc_.set_friendly_name(FLAGS_name);

  resource_desc_.set_task_capacity(0);
}

void Worker::HandleIncomingMessage(BaseMessage *bm,
                                   const string& /*remote_endpoint*/) {
  // Registration message
  if (bm->has_registration()) {
    LOG(ERROR) << "Received registration message, but workers cannot have any "
               << "remote resources registered with them! Ignoring.";
  }
  // Heartbeat message
  if (bm->has_heartbeat()) {
    LOG(ERROR) << "Received heartbeat message, but workers cannot have any "
               << "remote resources registered with them! Ignoring.";
  }
}

bool Worker::RegisterWithCoordinator() {
  BaseMessage bm;
  ResourceDescriptor* rd = bm.mutable_registration()->mutable_res_desc();
  *rd = resource_desc_;  // copies current local RD!
  SUBMSG_WRITE(bm, registration, uuid, to_string(uuid_));
  // wrap in envelope
  VLOG(2) << "Sending registration message...";
  // send heartbeat message
  return SendMessageToCoordinator(&bm);
}

void Worker::SendHeartbeat() {
  BaseMessage bm;
  SUBMSG_WRITE(bm, registration, uuid, to_string(uuid_));
  // TODO(malte): we do not always need to send the location string; it
  // sufficies to send it if our location changed (which should be rare).
  SUBMSG_WRITE(bm, heartbeat, location, chan_->LocalEndpointString());
  SUBMSG_WRITE(bm, heartbeat, capacity, 1);
  // TODO(malte): report how many free resources we have
  SUBMSG_WRITE(bm, heartbeat, load, 1);
  VLOG(1) << "Sending heartbeat  message!";
  SendMessageToCoordinator(&bm);
}

bool Worker::SendMessageToCoordinator(BaseMessage* msg) {
  return SendMessageToRemote(chan_, msg);
}

bool Worker::ConnectToCoordinator(const string& coordinator_uri) {
  if (!ConnectToRemote(coordinator_uri, chan_))
    return false;
  // Send registration message
  return RegisterWithCoordinator();
}

ResourceID_t Worker::GenerateUUID() {
  boost::uuids::random_generator gen;
  return gen();
}

void Worker::Run() {
  // Worker starting -- first need to find a coordinator and connect to it.
  if (coordinator_uri_.empty()) {
    if (!RunCoordinatorDiscovery(coordinator_uri_)) {
      LOG(FATAL) << "No coordinator URI set, and automatic coordinator "
                 << "discovery failed! Exiting...";
    }
  }

  // We now know where the coordinator is. Establish a connection to it.
  CHECK(ConnectToCoordinator(coordinator_uri_))
      << "Failed to connect to the coordinator at " + coordinator_uri_;

  while (!exit_) {  // main loop
    // Wait for events
    SendHeartbeat();
    // TODO(malte): What we want here is a select() semantic, i.e. wait for a
    // message for up to N seconds, and if we haven't received one, go round the
    // loop to heartbeat again.
    //AwaitNextMessage();
    boost::this_thread::sleep(seconds(10));
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; terminate running
  // tasks etc.
  VLOG(1) << "Dropped out of main loop -- cleaning up...";
}

bool Worker::RunCoordinatorDiscovery(const string& coordinator_uri) {
  LOG(FATAL) << "Coordinator auto-discovery is not implemented yet. "
             << "coordinator_uri given was: " << coordinator_uri;
  return false;
}

}  // namespace firmament
