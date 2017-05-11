/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

// Main task library class.
// TODO(malte): This should really be made platform-independent, so that we can
// have platform-specific libraries.

#include "engine/task_lib.h"

#include <vector>
#include <stdlib.h>
#include <iostream>
#include <unistd.h>
#include <string>

#include "base/common.h"
#include "base/data_object.h"
#include "base/units.h"
#include "messages/registration_message.pb.h"
#include "messages/task_heartbeat_message.pb.h"
#include "messages/task_info_message.pb.h"
#include "messages/task_spawn_message.pb.h"
#include "messages/task_state_message.pb.h"
#include "misc/utils.h"
#include "platforms/common.h"


DEFINE_string(coordinator_uri, "", "The URI to contact the coordinator at.");
DEFINE_string(resource_id, "",
        "The resource ID that is running this task.");
DEFINE_string(task_id, "", "The ID of this task.");
DEFINE_uint64(heartbeat_interval, 1000000,
        "The interval, in microseconds, between heartbeats sent to the"
        "coordinator.");

DEFINE_string(tasklib_application, "",
              "The application running alongside tasklib");

#define SET_PROTO_IF_DICT_HAS_INT(proto, dict, member, val) \
  val = json_object_get(dict, # member); \
  if (val) proto->set ## _ ## member(json_integer_value(val));

#define SET_PROTO_IF_DICT_HAS_DOUBLE(proto, dict, member, val) \
  val = json_object_get(dict, # member); \
  if (val) proto->set ## _ ## member(json_real_value(val));


namespace firmament {

TaskLib::TaskLib()
  : m_adapter_(new StreamSocketsAdapter<BaseMessage>()),
    chan_(new StreamSocketsChannel<BaseMessage>(
        StreamSocketsChannel<BaseMessage>::SS_TCP)),
    coordinator_uri_(""),
    resource_id_(GenerateResourceID()),
    pid_(getpid()),
    task_running_(false),
    heartbeat_seq_number_(0),
    stop_(false),
    internal_completed_(false),
    completed_(0),
    task_perf_monitor_(1000000) {
  hostname_ = boost::asio::ip::host_name();

  char* coord_uri_env = getenv("FLAGS_coordinator_uri");
  if (coord_uri_env)
    coordinator_uri_ = coord_uri_env;
  else
    LOG(ERROR) << "No coordinator_uri environment variable!";

  const char* task_id_env = getenv("FLAGS_task_id");
  VLOG(1) << "Task ID is " << task_id_env;
  CHECK_NOTNULL(task_id_env);
  task_id_ = TaskIDFromString(task_id_env);

  char* res_id_env = getenv("FLAGS_resource_id");
  if (res_id_env)
    resource_id_ = ResourceIDFromString(res_id_env);

  stringstream ss;
  ss << "/tmp/" << task_id_env << ".pid";
  string pid_filename = ss.str();

  FILE* pid_file;
  pid_file = fopen(pid_filename.c_str(), "w");
  if (!pid_file) {
    PLOG(ERROR) << "Failed to create PID file (" << pid_filename << ")";
  } else {
    int pid = getpid();
    fprintf(pid_file, "%d", pid);
    CHECK_EQ(fclose(pid_file), 0);
  }

  use_procfs_ = true;
}

TaskLib::~TaskLib() {
}

void TaskLib::Stop(bool success) {
  // This may get called several times by different threads; we only want to
  // stop once!
  if (!stop_) {
    LOG(INFO) << "STOP CALLED";
    stop_ = true;
    //while (task_running_) {
    //  boost::this_thread::sleep(boost::posix_time::milliseconds(50));
    //   //Wait until the monitor has stopped before sending the finalize
    //   //message.
    //}
    sleep(1);
    if (chan_) {
      LOG(INFO) << "Sending finalize message to coordinator...";
      SendFinalizeMessage(success);
      LOG(INFO) << "Finalise message sent";
    } else {
      LOG(ERROR) << "Task exited before a channel to the coordinator could be "
                 << "established!";
    }
    fflush(stdout);
    fflush(stderr);
    // Remove PID file
    stringstream ss;
    ss << "/tmp/" << task_id_ << ".pid";
    string pid_filename = ss.str();
    unlink(pid_filename.c_str());
    //exit(0);
  }
}

void TaskLib::AddTaskStatisticsToHeartbeat(
    const ProcFSMonitor::ProcessStatistics_t& proc_stats, TaskStats* stats) {
  // Task ID and timestamp
  stats->set_task_id(task_id_);
  stats->set_hostname(hostname_);
  stats->set_timestamp(time_manager_.GetCurrentTimestamp());

  if (use_procfs_) {
    // Memory allocated and used
    stats->set_mem_usage(proc_stats.rss * getpagesize() / BYTES_TO_KB);
  }
}

void TaskLib::SetCompleted(double completed) {
  completed_ = completed;
  // Mark us as getting updates within the internals.
  internal_completed_ = true;
}

bool TaskLib::ConnectToCoordinator(const string& coordinator_uri) {
  return m_adapter_->EstablishChannel(coordinator_uri, chan_);
}

void TaskLib::Spawn(const ReferenceInterface& code,
                    vector<ReferenceInterface>* /*dependencies*/,
                    vector<FutureReference>* outputs) {
  VLOG(1) << "Spawning a new task; code reference is " << code.desc().id();
  // Craft a task spawn message for the new task, using a newly created task
  // descriptor.
  BaseMessage msg;
  SUBMSG_WRITE(msg, task_spawn, creating_task_id, task_id_);
  //TaskDescriptor* new_task =
  //    msg.mutable_task_spawn()->mutable_spawned_task_desc();
  TaskDescriptor* new_task = task_descriptor_.add_spawned();
  new_task->set_uid(GenerateTaskID(task_descriptor_));
  VLOG(1) << "New task's ID is " << new_task->uid();
  new_task->set_name("");
  new_task->set_state(TaskDescriptor::CREATED);
  // XXX(malte): set the code ref of the new task
  //new_task->set_binary(code);
  // Create the outputs of the new task
  uint64_t i = 0;
  for (vector<FutureReference>::iterator out_iter = outputs->begin();
       out_iter != outputs->end();
       ++out_iter) {
    DataObjectID_t new_output_id = GenerateDataObjectID(*new_task);
    VLOG(1) << "Output " << i << "'s ID: " << new_output_id;
    ReferenceDescriptor* out_rd = new_task->add_outputs();
    out_rd->CopyFrom(out_iter->desc());
    out_rd->set_id(new_output_id.name_bytes(), DIOS_NAME_BYTES);
    out_rd->set_producing_task(new_task->uid());
  }
  // Job ID field must be set on task spawn
  CHECK(!task_descriptor_.job_id().empty());
  new_task->set_job_id(task_descriptor_.job_id());
  // Copy the new task descriptor into the message
  msg.mutable_task_spawn()->mutable_spawned_task_desc()->CopyFrom(*new_task);
  // Off we go!
  SendMessageToCoordinator(&msg);
}

void TaskLib::Publish(const vector<ConcreteReference>& /*references*/) {
  LOG(ERROR) << "Output publication currently unimplemented!";
}

void TaskLib::ConvertTaskArgs(int argc, char *argv[], vector<char*>* arg_vec) {
  VLOG(1) << "Stripping Firmament default arguments...";
  for (int64_t i = 0; i < argc; ++i) {
    if (strstr(argv[i], "--tryfromenv") || strstr(argv[i], "--v")) {
      // Ignore standard arguments that refer to Firmament's task_lib, rather
      // than being passed through to the user code.
      continue;
    } else {
      arg_vec->push_back(argv[i]);
    }
    VLOG(1) << arg_vec->size() << " out of " << argc << " original arguments "
            << "remain.";
  }
}

void TaskLib::HandleIncomingMessage(BaseMessage *bm,
                                    const string& remote_endpoint) {
  LOG(INFO) << "Got message from " << remote_endpoint << ": "
            << bm->DebugString();
  // Task kill message
  if (bm->has_task_kill()) {
    const TaskKillMessage& msg = bm->task_kill();
    LOG(ERROR) << "Received task kill request from " << remote_endpoint
               << ", terminating due to " << msg.reason();
    Stop(false);
    exit(1);
  }
}

void TaskLib::HandleWrite(const boost::system::error_code& error,
        size_t bytes_transferred) {
  VLOG(1) << "In HandleWrite, thread is " << boost::this_thread::get_id();
  if (error)
    LOG(ERROR) << "Error returned from async write: " << error.message();
  else
    VLOG(1) << "bytes_transferred: " << bytes_transferred;
}

bool TaskLib::PullTaskInformationFromCoordinator(TaskID_t task_id,
                                                 TaskDescriptor* desc) {
  // Send request for task information to coordinator
  BaseMessage msg;
  SUBMSG_WRITE(msg, task_info_request, task_id, task_id);
  SUBMSG_WRITE(msg, task_info_request, requesting_resource_id,
               to_string(resource_id_));
  SUBMSG_WRITE(msg, task_info_request, requesting_endpoint,
               chan_->LocalEndpointString());
  Envelope<BaseMessage> envelope(&msg);
  CHECK(chan_->SendS(envelope));
  // Wait for a response
  BaseMessage response;
  Envelope<BaseMessage> recv_envelope(&response);
  chan_->RecvS(&recv_envelope);
  CHECK(response.has_task_info_response());
  desc->CopyFrom(SUBMSG_READ(response, task_info_response, task_desc));
  VLOG(1) << "Received TD: " << desc->DebugString();
  return true;
}


void TaskLib::RunMonitor(boost::thread::id main_thread_id) {
  FLAGS_logtostderr = true;
  if (coordinator_uri_.empty())
    coordinator_uri_ = FLAGS_coordinator_uri;

  LOG(INFO) << "Connecting to coordinator at " << coordinator_uri_;
  CHECK(ConnectToCoordinator(coordinator_uri_));

  m_adapter_->RegisterAsyncMessageReceiptCallback(
      boost::bind(&TaskLib::HandleIncomingMessage, this, _1, _2));
  //m_adapter_->RegisterAsyncErrorPathCallback(
  //        boost::bind(&TaskLib::HandleIncomingReceiveError, this,
  //        boost::asio::placeholders::error, _2));

  //VLOG(3) << "Setting up storage engine";
  //setUpStorageEngine();
  //VLOG(2) << "Finished setting up storage engine";

  task_running_ = true;
  VLOG(3) << "Setting up process statistics\n";

  ProcFSMonitor::ProcessStatistics_t current_stats;
  bzero(&current_stats, sizeof(ProcFSMonitor::ProcessStatistics_t));
  VLOG(3) << "Finished setting up process statistics\n";

  // This will check if the task thread has joined once every heartbeat
  // interval, and go back to sleep if it has not.
  // TODO(malte): think about whether we'd like some kind of explicit
  // notification scheme in case the heartbeat interval is large.

  while (!stop_) {
    // TODO(malte): Check if we've exited with an error
    // if(error)
    //   task_error_ = true;
    // Notify the coordinator that we're still running happily

    VLOG(1) << "Task thread has not yet joined, sending heartbeat...";

    if (use_procfs_) {
      task_perf_monitor_.ProcessInformation(pid_, &current_stats);
    }
    SendHeartbeat(current_stats);

    // TODO(malte): We'll need to receive any potential messages from the
    // coordinator here, too. This is probably best done by a simple RecvA on
    // the channel.
    m_adapter_->AwaitNextMessage();

    // Finally, nap for a bit until the next heartbeat is due
    usleep(FLAGS_heartbeat_interval);
  }
  LOG(INFO) << "STOPPING HEARTBEATS for " << pid_;
  fflush(stderr);
  task_running_ = false;
}

void TaskLib::SendFinalizeMessage(bool success) {
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_state, id, task_id_);
  if (success)
    SUBMSG_WRITE(bm, task_state, new_state, TaskDescriptor::COMPLETED);
  else
    SUBMSG_WRITE(bm, task_state, new_state, TaskDescriptor::ABORTED);
  LOG(INFO) << "Sending finalize message (task state change to "
            << (success ? "COMPLETED" : "ABORTED") << ")!";
  //SendMessageToCoordinator(&bm);
  Envelope<BaseMessage> envelope(&bm);
  CHECK(chan_->SendS(envelope));
  LOG(INFO) << "Done sending message, sleeping before quitting";
  boost::this_thread::sleep(boost::posix_time::seconds(1));
}

void TaskLib::SendHeartbeat(
  const ProcFSMonitor::ProcessStatistics_t& proc_stats) {
  BaseMessage bm;
  SUBMSG_WRITE(bm, task_heartbeat, task_id, task_id_);
  // Add current set of procfs statistics

  TaskStats* task_stats = bm.mutable_task_heartbeat()->mutable_stats();
  AddTaskStatisticsToHeartbeat(proc_stats, task_stats);

  // TODO(malte): we do not always need to send the location string; it
  // sufficies to send it if our location changed (which should be rare).
  SUBMSG_WRITE(bm, task_heartbeat, location, chan_->LocalEndpointString());
  SUBMSG_WRITE(bm, task_heartbeat, sequence_number, heartbeat_seq_number_++);

  //LOG(INFO) << "Sending heartbeat message!";
  SendMessageToCoordinator(&bm);
}

bool TaskLib::SendMessageToCoordinator(BaseMessage* msg) {
  Envelope<BaseMessage> envelope(msg);
  return chan_->SendS(envelope);
  /*return chan_->SendA(
          envelope, boost::bind(&TaskLib::HandleWrite,
          this,
          boost::asio::placeholders::error,
          boost::asio::placeholders::bytes_transferred));*/
}

} // namespace firmament
