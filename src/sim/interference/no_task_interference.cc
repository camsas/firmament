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

#include "sim/interference/no_task_interference.h"

#include "misc/map-util.h"
#include "misc/utils.h"
#include "sim/simulator_utils.h"

DECLARE_uint64(runtime);
DECLARE_double(trace_speed_up);

namespace firmament {
namespace sim {

NoTaskInterference::NoTaskInterference(
    unordered_map<TaskID_t, uint64_t>* task_runtime)
  : task_runtime_(task_runtime) {
}

NoTaskInterference::~NoTaskInterference() {
  // The object doesn't own task_runtime_.
}

void NoTaskInterference::OnTaskCompletion(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
}

void NoTaskInterference::OnTaskEviction(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  task_end_runtimes.set_previous_end_time(td_ptr->finish_time());
  uint64_t task_executed_for = current_time_us - td_ptr->start_time();
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until eviction is
    // saved. Hence, we update the time the task has left to run.
    InsertOrUpdate(task_runtime_, task_id, *runtime_ptr - task_executed_for);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->clear_start_time();
  td_ptr->set_submit_time(current_time_us);
  tasks_end_time->push_back(task_end_runtimes);
}

void NoTaskInterference::OnTaskMigration(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t old_res_id,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  TaskID_t task_id = td_ptr->uid();
  uint64_t task_executed_for = current_time_us - td_ptr->start_time();
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until migration is
    // saved. Hence, we update the time the task has left to run.
    InsertOrUpdate(task_runtime_, task_id, *runtime_ptr - task_executed_for);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->set_submit_time(current_time_us);
  td_ptr->set_start_time(current_time_us);
  // We don't have to update the end event because we assume that it
  // takes no time to migrate a task.
}

void NoTaskInterference::OnTaskPlacement(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  td_ptr->set_start_time(current_time_us);
  td_ptr->set_total_unscheduled_time(UpdateTaskTotalUnscheduledTime(*td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, td_ptr->uid());
  if (runtime_ptr != NULL) {
    // We can approximate the duration of the task.
    td_ptr->set_finish_time(current_time_us + *runtime_ptr);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // the timestamp just after the end of the simulation.
    td_ptr->set_finish_time(FLAGS_runtime / FLAGS_trace_speed_up + 1);
  }
  task_end_runtimes.set_current_end_time(td_ptr->finish_time());
  tasks_end_time->push_back(task_end_runtimes);
}

}  // namespace sim
}  // namespace firmament
