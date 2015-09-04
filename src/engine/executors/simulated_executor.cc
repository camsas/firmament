// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "engine/executors/simulated_executor.h"

#include <string>
#include <vector>

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

void SimulatedExecutor::HandleTaskCompletion(TaskDescriptor* td_ptr,
                                             TaskFinalReport* task_report) {
  // NOTE: We do not have information to set instructions, cycles, llc_refs
  // and llc_misses.
  task_report->set_task_id(td_ptr->uid());
  task_report->set_start_time(td_ptr->start_time());
  task_report->set_finish_time(td_ptr->finish_time());
  task_report->set_runtime(td_ptr->finish_time() -
                           td_ptr->start_time());
}

void SimulatedExecutor::HandleTaskEviction(TaskDescriptor* td_ptr) {
  // No-op.
  // The simulator is responsible for updating the task's state.
}

void SimulatedExecutor::HandleTaskFailure(TaskDescriptor* td_ptr) {
  // No-op.
  // The simulator is responsible for updating the task's state.
}

void SimulatedExecutor::RunTask(TaskDescriptor* td_ptr,
                                bool firmament_binary) {
  // No-op.
  // The simulator is responsible for setting task start time.
}

}  // namespace executor
}  // namespace firmament
