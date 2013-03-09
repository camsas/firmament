// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Remote executor class.

#include "engine/remote_executor.h"

#include "base/common.h"
#include "misc/map-util.h"

namespace firmament {
namespace executor {

RemoteExecutor::RemoteExecutor(
    ResourceID_t resource_id,
    const string& coordinator_uri,
    ResourceMap_t* res_map,
    MessagingAdapterInterface<BaseMessage>* m_adapter_ptr)
    : managing_coordinator_uri_(coordinator_uri),
      remote_resource_id_(resource_id),
      res_map_ptr_(res_map),
      m_adapter_ptr_(m_adapter_ptr) {
}

void RemoteExecutor::RunTask(TaskDescriptor* td, bool firmament_binary) {
  ResourceStatus** rs_ptr = FindOrNull(*res_map_ptr_, remote_resource_id_);
  CHECK(rs_ptr) << "Resource " << remote_resource_id_ << " appears to no "
                << "longer exist in the resource map!";
  const string remote_endpoint = (*rs_ptr)->location();
  VLOG(1) << "Remote task spawn on resource " << remote_resource_id_
          << ", endpoint " << remote_endpoint;
  shared_ptr<MessagingChannelInterface<BaseMessage> > chan =
      m_adapter_ptr_->GetChannelForEndpoint(remote_endpoint);
  VLOG(1) << "chan at " << chan;
}

}  // namespace executor
}  // namespace firmament
