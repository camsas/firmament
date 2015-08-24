// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_ENGINE_SIMULATED_EXECUTOR_H
#define FIRMAMENT_ENGINE_SIMULATED_EXECUTOR_H

#include <string>
#include <vector>

#include "base/common.h"
#include "base/task_final_report.pb.h"
#include "engine/executor_interface.h"

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

#endif  // FIRMAMENT_ENGINE_SIMULATED_EXECUTOR_H
