// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Stub header for remote executor.

#ifndef FIRMAMENT_ENGINE_REMOTE_EXECUTOR_H
#define FIRMAMENT_ENGINE_REMOTE_EXECUTOR_H

#include <vector>
#include <string>

#include "base/common.h"
#include "base/types.h"
#include "engine/executor_interface.h"

namespace firmament {
namespace executor {

class RemoteExecutor : public ExecutorInterface {
 public:
  explicit RemoteExecutor(ResourceID_t resource_id);
  bool RunTask(shared_ptr<TaskDescriptor> td);
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<RemoteExecutor for resource "
                   << to_string(local_resource_id_)
                   << ">";
  }
 protected:
  ResourceID_t local_resource_id_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_REMOTE_EXECUTOR_H
