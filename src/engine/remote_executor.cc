// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Remote executor class.

#include "engine/remote_executor.h"

#include "base/common.h"
#include "misc/protobuf_envelope.h"
#include "misc/map-util.h"
#include "messages/base_message.pb.h"
#include "messages/task_delegation_message.pb.h"

namespace firmament {
namespace executor {

RemoteExecutor::RemoteExecutor(
    ResourceID_t resource_id,
    ResourceID_t coordinator_resource_id,
    const string& coordinator_uri,
    ResourceMap_t* res_map,
    MessagingAdapterInterface<BaseMessage>* m_adapter_ptr)
    : managing_coordinator_uri_(coordinator_uri),
      remote_resource_id_(resource_id),
      local_resource_id_(coordinator_resource_id),
      res_map_ptr_(res_map),
      m_adapter_ptr_(m_adapter_ptr) {
}

void RemoteExecutor::RunTask(TaskDescriptor* td, bool firmament_binary) {
  MessagingChannelInterface<BaseMessage>* chan = GetChannel();
  SendTaskExecutionMessage(chan, td, firmament_binary);
}

MessagingChannelInterface<BaseMessage>* RemoteExecutor::GetChannel() {
  ResourceStatus** rs_ptr = FindOrNull(*res_map_ptr_, remote_resource_id_);
  CHECK(rs_ptr) << "Resource " << remote_resource_id_ << " appears to no "
                << "longer exist in the resource map!";
  const string remote_endpoint = (*rs_ptr)->location();
  VLOG(1) << "Remote task spawn on resource " << remote_resource_id_
          << ", endpoint " << remote_endpoint;
  MessagingChannelInterface<BaseMessage>* chan =
      m_adapter_ptr_->GetChannelForEndpoint(remote_endpoint);
  VLOG(1) << "Chan is " << chan;
  return chan;
}

bool RemoteExecutor::SendTaskExecutionMessage(
    MessagingChannelInterface<BaseMessage>* chan,
    TaskDescriptor* td, bool firmament_binary) {
  CHECK_NOTNULL(chan);
  // Craft a task execution message
  BaseMessage exec_message;
  TaskDescriptor* msg_td =
      exec_message.MutableExtension(task_delegation_extn)->
      mutable_task_descriptor();
  // N.B. copies task descriptor
  msg_td->CopyFrom(*td);
  SUBMSG_WRITE(exec_message, task_delegation, target_resource_id,
               to_string(remote_resource_id_));
  SUBMSG_WRITE(exec_message, task_delegation, delegating_resource_id,
               to_string(local_resource_id_));
  Envelope<BaseMessage> envelope(&exec_message);
  // Send it to the relevant resource's coordinator
  CHECK(chan->SendS(envelope));
  // Receive the response
  //chan->RecvS();
  return true;
}

}  // namespace executor
}  // namespace firmament
