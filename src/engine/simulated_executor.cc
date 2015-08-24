// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "engine/simulated_executor.h"

#include "base/units.h"

namespace firmament {
namespace executor {

SimulatedExecutor::SimulatedExecutor(ResourceID_t resource_id,
                                     const string& coordinator_uri) :
    coordinator_uri_(coordinator_uri), simulated_resource_id_(resource_id) {
}

bool SimulatedExecutor::CheckRunningTasksHealth(
    vector<TaskID_t>* failed_tasks) {
  return true;
}

void SimulatedExecutor::HandleTaskCompletion(TaskDescriptor* td,
                                             TaskFinalReport* report) {
  // The simulator is responsible for updating the task's state.
}

void SimulatedExecutor::HandleTaskEviction(TaskDescriptor* td) {
  // The simulator is responsible for updating the task's state.
}

void SimulatedExecutor::HandleTaskFailure(TaskDescriptor* td) {
  // The simulator is responsible for updating the task's state.
}

void SimulatedExecutor::RunTask(TaskDescriptor* td,
                                bool firmament_binary) {
  // The simulator is responsible for setting task start time.
}

}  // namespace executor
}  // namespace firmament
