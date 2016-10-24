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

#ifndef FIRMAMENT_ENGINE_EXECUTORS_SIMULATED_EXECUTOR_H
#define FIRMAMENT_ENGINE_EXECUTORS_SIMULATED_EXECUTOR_H

#include "engine/executors/executor_interface.h"

#include <string>
#include <vector>

#include "base/common.h"
#include "base/task_final_report.pb.h"

namespace firmament {
namespace executor {

class SimulatedExecutor : public ExecutorInterface {
 public:
  SimulatedExecutor(ResourceID_t resource_id,
                    const string& coordinator_uri);
  bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks);
  void HandleTaskCompletion(TaskDescriptor* td,
                            TaskFinalReport* report);
  void HandleTaskEviction(TaskDescriptor* td);
  void HandleTaskFailure(TaskDescriptor* td);
  void RunTask(TaskDescriptor* td,
               bool firmament_binary);
  ostream& ToString(ostream* stream) const {
    return *stream << "<SimlatedExecutor at resource "
                   << to_string(simulated_resource_id_) << ">";
  }

 private:
  string coordinator_uri_;
  ResourceID_t simulated_resource_id_;
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_SIMULATED_EXECUTOR_H
