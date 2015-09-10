// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub header for remote executor.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_REMOTE_EXECUTOR_H
#define FIRMAMENT_ENGINE_EXECUTORS_REMOTE_EXECUTOR_H

#include "engine/executors/executor_interface.h"

#include <vector>
#include <string>

#include "base/common.h"
#include "misc/messaging_interface.h"
#include "messages/base_message.pb.h"

namespace firmament {
namespace executor {

class RemoteExecutor : public ExecutorInterface {
 public:
  RemoteExecutor(ResourceID_t resource_id,
                 ResourceID_t coordinator_resource_id,
                 const string& coordinator_uri,
                 ResourceMap_t* res_map,
                 MessagingAdapterInterface<BaseMessage>* m_adapter_ptr);
  bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks);
  void HandleTaskCompletion(TaskDescriptor* td,
                            TaskFinalReport* report);
  void HandleTaskEviction(TaskDescriptor* td);
  void HandleTaskFailure(TaskDescriptor* td);
  void RunTask(TaskDescriptor* td,
               bool firmament_binary);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<RemoteExecutor for resource "
                   << to_string(remote_resource_id_)
                   << ", managed by coordinator at "
                   << managing_coordinator_uri_
                   << ">";
  }

 protected:
  const string managing_coordinator_uri_;
  ResourceID_t remote_resource_id_;
  ResourceID_t local_resource_id_;
  ResourceMap_t* res_map_ptr_;
  MessagingAdapterInterface<BaseMessage>* m_adapter_ptr_;

  MessagingChannelInterface<BaseMessage>* GetChannel();
  void SendTaskExecutionMessage(
    MessagingChannelInterface<BaseMessage>* chan,
    TaskDescriptor* td, bool firmament_binary);
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_REMOTE_EXECUTOR_H
