// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Main executor class.
// TODO(malte): This should really be made platform-independent, so that we can
// have platform-specific executor libraries.

#include "engine/executor.h"

#include "base/common.h"
#include "messages/heartbeat_message.pb.h"
#include "messages/registration_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "platforms/common.h"
#include "platforms/common.pb.h"

DEFINE_string(coordinator_uri, "", "The URI to contact the coordinator at.");
DEFINE_string(resource_id, "", "The resource ID that is running this executor.");

namespace firmament {

Executor::Executor()
  : m_adapter_(new StreamSocketsAdapter<BaseMessage>()),
    chan_(StreamSocketsChannel<BaseMessage>::SS_TCP),
    coordinator_uri_(FLAGS_coordinator_uri),
    resource_id_(ResourceIDFromString(FLAGS_resource_id)),
    task_running_(false) {
}

void Executor::AwaitNextMessage() {
  // Finally, call back into ourselves.
  //AwaitNextMessage();
}

bool Executor::RegisterWithCoordinator() {
  // XXX(malte): stub
  return false;
}

void Executor::SendFinalizeMessage(bool success) {
  BaseMessage bm;
  if (success)
    SUBMSG_WRITE(bm, task_state, new_state, TaskDescriptor::COMPLETED);
    //bm.MutableExtension(task_state_extn)->set_state(TaskDescriptor::COMPLETED);
  else
    LOG(FATAL) << "Unimplemented error path!";
  VLOG(1) << "Sending finalize message (task state change to "
          << (success ? "COMPLETED" : "FAILED") << "!";
  //SendMessageToCoordinator(&bm);
  Envelope<BaseMessage> envelope(&bm);
  CHECK(chan_.SendS(envelope));
  VLOG(1) << "Done sending message, sleeping before quitting";
  //boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void Executor::SendHeartbeat() {
  BaseMessage bm;
  bm.MutableExtension(heartbeat_extn)->set_uuid(
      boost::uuids::to_string(resource_id_));
  // TODO(malte): we do not always need to send the location string; it
  // sufficies to send it if our location changed (which should be rare).
  SUBMSG_WRITE(bm, heartbeat, location, chan_.LocalEndpointString());
  SUBMSG_WRITE(bm, heartbeat, sequence_number, 1);
  VLOG(1) << "Sending heartbeat message!";
  SendMessageToCoordinator(&bm);
}

bool Executor::SendMessageToCoordinator(BaseMessage* msg) {
  Envelope<BaseMessage> envelope(msg);
  return chan_.SendA(
      envelope, boost::bind(&Executor::HandleWrite,
                            this,
                            boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred));
}

bool Executor::ConnectToCoordinator(const string& coordinator_uri) {
  return m_adapter_->EstablishChannel(coordinator_uri, &chan_);
}

void Executor::HandleWrite(const boost::system::error_code& error,
                         size_t bytes_transferred) {
  VLOG(1) << "In HandleWrite, thread is " << boost::this_thread::get_id();
  if (error)
    LOG(ERROR) << "Error returned from async write: " << error.message();
  else
    VLOG(1) << "bytes_transferred: " << bytes_transferred;
}

void Executor::Run() {
  // TODO(malte): Any setup work goes here
  CHECK(ConnectToCoordinator(coordinator_uri_))
      << "Failed to connect to coordinator; is it reachable?";

  // Async receive -- the handler is responsible for invoking this again.
  AwaitNextMessage();

  // Run task
  task_running_ = true;
  RunTask();

  while (task_running_) {  // main loop -- && task_running
    // Send period heartbeats
    SendHeartbeat();
    VLOG(1) << "Sent heartbeat...";
    boost::this_thread::sleep(boost::posix_time::seconds(1));
  }

  // We have dropped out of the main loop and are exiting
  // TODO(malte): any cleanup we need to do; terminate running
  // tasks etc.
  VLOG(1) << "Dropped out of main loop -- cleaning up...";
  // XXX(malte): Deal with error case!
  SendFinalizeMessage(true);
}

void Executor::RunTask() {
  // TODO(malte): execute the actual task code
  //CHECK(task_desc_.code_dependency.is_consumable());
  //fork();
  //if (child) {
  LOG(INFO) << "Invoking task code...";
  char *task_id_env = getenv("TASK_ID");
  CHECK_NOTNULL(task_id_env);
  // task_main blocks until the task has exited
  task_main(atol(task_id_env));
  task_running_ = false;
  //  exec(task_desc_.code_dependency());
  //} else {
  //  task_running_ = true;
  //  join();
  //  task_running_ = false;
  //}
}

}  // namespace firmament
