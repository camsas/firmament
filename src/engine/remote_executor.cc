// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Remote executor class.

#include "engine/remote_executor.h"

#include "base/common.h"

namespace firmament {
namespace executor {

RemoteExecutor::RemoteExecutor(ResourceID_t resource_id,
                               const string& coordinator_uri)
    : managing_coordinator_uri_(coordinator_uri),
      remote_resource_id_(resource_id) {
}

void RemoteExecutor::RunTask(TaskDescriptor* td, bool firmament_binary) {
  LOG(FATAL) << "Unimplemented!";
}

}  // namespace executor
}  // namespace firmament
