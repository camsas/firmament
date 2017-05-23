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

#include "sim/interference/quincy_task_interference.h"

#include "misc/map-util.h"
#include "misc/utils.h"
#include "sim/simulator_utils.h"

DEFINE_double(quincy_interference_runtime_increase, 0.1,
              "The fraction by which task runtime increases when another task "
              "is co-located");

DECLARE_uint64(runtime);
DECLARE_double(trace_speed_up);

namespace firmament {
namespace sim {

QuincyTaskInterference::QuincyTaskInterference(
    scheduler::SchedulerInterface* scheduler,
    multimap<ResourceID_t, ResourceDescriptor*>* machine_res_id_pus,
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_map<TaskID_t, uint64_t>* task_runtime)
  : scheduler_(scheduler),
    machine_res_id_pus_(machine_res_id_pus),
    resource_map_(resource_map),
    task_map_(task_map),
    task_runtime_(task_runtime) {
}

QuincyTaskInterference::~QuincyTaskInterference() {
  // The object doesn't own scheduler_, machine_res_id_pus_ and task_runtime_.
}

void QuincyTaskInterference::OnTaskCompletion(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  vector<TaskID_t> colocated_on_pu;
  vector<TaskID_t> colocated_on_machine;
  // co-located_on_pu does not include the completed task.
  GetColocatedTasks(res_id, &colocated_on_pu, &colocated_on_machine);
  uint64_t num_tasks_colocated =
    colocated_on_pu.size() + colocated_on_machine.size();
  UpdateOtherTasksOnMachine(current_time_us, num_tasks_colocated + 1,
                            num_tasks_colocated, colocated_on_pu,
                            colocated_on_machine, tasks_end_time);
}

void QuincyTaskInterference::OnTaskEviction(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  // Update runtime of the other co-located tasks.
  vector<TaskID_t> colocated_on_pu;
  vector<TaskID_t> colocated_on_machine;
  // co-located_on_pu does not include the evicted task.
  GetColocatedTasks(res_id, &colocated_on_pu, &colocated_on_machine);
  uint64_t num_tasks_colocated =
    colocated_on_pu.size() + colocated_on_machine.size();
  UpdateOtherTasksOnMachine(current_time_us, num_tasks_colocated + 1,
                            num_tasks_colocated, colocated_on_pu,
                            colocated_on_machine, tasks_end_time);
  // Update the evicted task.
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  task_end_runtimes.set_previous_end_time(td_ptr->finish_time());
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // NOTE: We assume that the work conducted by a task until eviction is
    // saved. Hence, we update the time the task has left to run.
    uint64_t task_executed_for = current_time_us - td_ptr->start_time();
    uint64_t real_executed_for =
      TimeWithInterferenceToTraceTime(task_executed_for,
                                      num_tasks_colocated + 1);
    InsertOrUpdate(task_runtime_, task_id, *runtime_ptr - real_executed_for);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->clear_start_time();
  td_ptr->set_submit_time(current_time_us);
  tasks_end_time->push_back(task_end_runtimes);
}

void QuincyTaskInterference::OnTaskMigration(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t old_res_id,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  ResourceID_t old_machine_res_id =
    MachineResIDForResource(resource_map_, old_res_id);
  ResourceID_t new_machine_res_id =
    MachineResIDForResource(resource_map_, res_id);
  // Update runtime of the tasks running on the machine from which the task
  // has been migrated.
  vector<TaskID_t> colocated_on_pu;
  vector<TaskID_t> colocated_on_machine;
  // co-located_on_pu does not include the migrated task.
  GetColocatedTasks(old_res_id, &colocated_on_pu, &colocated_on_machine);
  uint64_t num_tasks_colocated_on_old_machine =
    colocated_on_pu.size() + colocated_on_machine.size() + 1;
  if (old_machine_res_id != new_machine_res_id) {
    // co-located_on_machine does not include the migrated task.
    UpdateOtherTasksOnMachine(current_time_us,
                              num_tasks_colocated_on_old_machine,
                              num_tasks_colocated_on_old_machine - 1,
                              colocated_on_pu, colocated_on_machine,
                              tasks_end_time);
  }
  // Update runtime of the tasks running on the machine on which the task
  // has been migrated.
  TaskID_t task_id = td_ptr->uid();
  colocated_on_pu.clear();
  colocated_on_machine.clear();
  // co-located_on_pu does include the migrated task.
  GetColocatedTasks(res_id, &colocated_on_pu, &colocated_on_machine);
  // Remove the migrated task.
  auto task_it = find(colocated_on_pu.begin(), colocated_on_pu.end(), task_id);
  CHECK(task_it != colocated_on_pu.end());
  colocated_on_pu.erase(task_it);
  uint64_t num_tasks_colocated_on_new_machine =
    colocated_on_pu.size() + colocated_on_machine.size();
  if (old_machine_res_id != new_machine_res_id) {
    UpdateOtherTasksOnMachine(current_time_us,
                              num_tasks_colocated_on_new_machine,
                              num_tasks_colocated_on_new_machine + 1,
                              colocated_on_pu, colocated_on_machine,
                              tasks_end_time);
  }
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    TaskEndRuntimes task_end_runtimes(task_id);
    task_end_runtimes.set_previous_end_time(td_ptr->finish_time());
    // NOTE: We assume that the work conducted by a task until migration is
    // saved. Hence, we update the time the task has left to run.
    uint64_t task_executed_for = current_time_us - td_ptr->start_time();
    uint64_t real_executed_for =
      TimeWithInterferenceToTraceTime(task_executed_for,
                                      num_tasks_colocated_on_old_machine);
    uint64_t real_time_left = *runtime_ptr - real_executed_for;
    InsertOrUpdate(task_runtime_, task_id, real_time_left);
    uint64_t task_end_time = current_time_us +
      TraceTimeToTimeWithInterference(real_time_left,
                                      num_tasks_colocated_on_new_machine + 1);
    task_end_runtimes.set_current_end_time(task_end_time);
    td_ptr->set_finish_time(task_end_time);
    tasks_end_time->push_back(task_end_runtimes);
  } else {
    // The task didn't finish in the trace.
  }
  td_ptr->set_submit_time(current_time_us);
  td_ptr->set_start_time(current_time_us);
}

void QuincyTaskInterference::OnTaskPlacement(
    uint64_t current_time_us,
    TaskDescriptor* td_ptr,
    ResourceID_t res_id,
    vector<TaskEndRuntimes>* tasks_end_time) {
  TaskID_t task_id = td_ptr->uid();
  vector<TaskID_t> colocated_on_pu;
  vector<TaskID_t> colocated_on_machine;
  // co-located_on_pu includes the newly placed task.
  GetColocatedTasks(res_id, &colocated_on_pu, &colocated_on_machine);
  // Remove the placed task.
  auto task_it = find(colocated_on_pu.begin(), colocated_on_pu.end(), task_id);
  CHECK(task_it != colocated_on_pu.end());
  colocated_on_pu.erase(task_it);
  uint64_t num_tasks_colocated =
    colocated_on_pu.size() + colocated_on_machine.size();
  UpdateOtherTasksOnMachine(current_time_us, num_tasks_colocated,
                            num_tasks_colocated + 1, colocated_on_pu,
                            colocated_on_machine, tasks_end_time);
  // And end time for newly placed task.
  tasks_end_time->push_back(
      UpdateEndTimeForPlacedTask(td_ptr, current_time_us,
                                 num_tasks_colocated + 1));
}

void QuincyTaskInterference::GetColocatedTasks(
    ResourceID_t pu_res_id,
    vector<TaskID_t>* colocated_on_pu,
    vector<TaskID_t>* colocated_on_machine) {
  // Get the ResourceID_t of the machine on which the task has been placed.
  ResourceID_t machine_res_id =
    MachineResIDForResource(resource_map_, pu_res_id);
  // Get machine's PUs
  pair<multimap<ResourceID_t, ResourceDescriptor*>::iterator,
       multimap<ResourceID_t, ResourceDescriptor*>::iterator> range_it =
    machine_res_id_pus_->equal_range(machine_res_id);
  for (; range_it.first != range_it.second; range_it.first++) {
    // Get the tasks running on the PU
    ResourceID_t cur_pu_res_id =
      ResourceIDFromString(range_it.first->second->uuid());
    vector<TaskID_t> colocated_task_ids =
      scheduler_->BoundTasksForResource(cur_pu_res_id);
    if (cur_pu_res_id == pu_res_id) {
      colocated_on_pu->insert(colocated_on_pu->end(),
                              colocated_task_ids.begin(),
                              colocated_task_ids.end());
    } else {
      colocated_on_machine->insert(colocated_on_machine->end(),
                                   colocated_task_ids.begin(),
                                   colocated_task_ids.end());
    }
  }
}

uint64_t QuincyTaskInterference::TimeWithInterferenceToTraceTime(
    uint64_t time,
    uint64_t num_tasks_colocated) {
  return static_cast<uint64_t>(round(
      time /
      (1 + FLAGS_quincy_interference_runtime_increase * num_tasks_colocated)));
}

uint64_t QuincyTaskInterference::TraceTimeToTimeWithInterference(
    uint64_t time,
    uint64_t num_tasks_colocated) {
  return static_cast<uint64_t>(round(
      time *
      (1 + FLAGS_quincy_interference_runtime_increase * num_tasks_colocated)));
}

TaskEndRuntimes QuincyTaskInterference::UpdateEndTimeForPlacedTask(
    TaskDescriptor* td_ptr,
    uint64_t current_time_us,
    uint64_t num_tasks_colocated) {
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  td_ptr->set_start_time(current_time_us);
  td_ptr->set_total_unscheduled_time(UpdateTaskTotalUnscheduledTime(*td_ptr));
  uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
  if (runtime_ptr != NULL) {
    // This assumes that task runtime increases by
    // quincy_interference_runtime_increase * 100 percent * num_tasks_colocated
    // when the task is colocated with num_tasks_colocated.
    uint64_t task_end_time = current_time_us +
      TraceTimeToTimeWithInterference(*runtime_ptr, num_tasks_colocated);
    task_end_runtimes.set_current_end_time(task_end_time);
    td_ptr->set_finish_time(task_end_time);
  } else {
    // The task didn't finish in the trace. Set the task's end event to the
    // the timestamp just after the end of the simulation.
    uint64_t task_end_time = FLAGS_runtime / FLAGS_trace_speed_up + 1;
    task_end_runtimes.set_current_end_time(task_end_time);
    td_ptr->set_finish_time(task_end_time);
  }
  return task_end_runtimes;
}

TaskEndRuntimes QuincyTaskInterference::UpdateEndTimeForRunningTask(
    TaskDescriptor* td_ptr,
    uint64_t sim_task_runtime,
    uint64_t current_time_us,
    uint64_t prev_num_tasks_colocated,
    uint64_t cur_num_tasks_colocated) {
  TaskID_t task_id = td_ptr->uid();
  TaskEndRuntimes task_end_runtimes(task_id);
  task_end_runtimes.set_previous_end_time(td_ptr->finish_time());
  td_ptr->set_total_run_time(ComputeTaskTotalRunTime(current_time_us, *td_ptr));
  uint64_t task_executed_for = current_time_us - td_ptr->start_time();
  // Transform the time task executed for under co-location interference
  // to the time tasks would have executed for in the simulation without
  // co-location interference.
  uint64_t real_executed_for =
    TimeWithInterferenceToTraceTime(task_executed_for,
                                    prev_num_tasks_colocated);
  // Update task_runtime_ to contain how much the task has left to execute if
  // it were to execute without interference.
  uint64_t time_left_to_execute = sim_task_runtime - real_executed_for;
  InsertOrUpdate(task_runtime_, task_id, time_left_to_execute);
  uint64_t task_end_time = current_time_us +
    TraceTimeToTimeWithInterference(time_left_to_execute,
                                    cur_num_tasks_colocated);
  td_ptr->set_start_time(current_time_us);
  task_end_runtimes.set_current_end_time(task_end_time);
  td_ptr->set_finish_time(task_end_time);
  return task_end_runtimes;
}

void QuincyTaskInterference::UpdateOtherTasksOnMachine(
    uint64_t current_time_us,
    uint64_t prev_num_tasks_colocated,
    uint64_t cur_num_tasks_colocated,
    const vector<TaskID_t>& colocated_on_pu,
    const vector<TaskID_t>& colocated_on_machine,
    vector<TaskEndRuntimes>* tasks_end_time) {
  for (auto& task_id : colocated_on_pu) {
    uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
    if (runtime_ptr != NULL) {
      TaskDescriptor* cur_td_ptr = FindPtrOrNull(*task_map_, task_id);
      CHECK_NOTNULL(cur_td_ptr);
      tasks_end_time->push_back(
          UpdateEndTimeForRunningTask(cur_td_ptr,
                                      *runtime_ptr,
                                      current_time_us,
                                      prev_num_tasks_colocated,
                                      cur_num_tasks_colocated));
    }
  }
  for (auto& task_id : colocated_on_machine) {
    uint64_t* runtime_ptr = FindOrNull(*task_runtime_, task_id);
    if (runtime_ptr != NULL) {
      TaskDescriptor* cur_td_ptr = FindPtrOrNull(*task_map_, task_id);
      CHECK_NOTNULL(cur_td_ptr);
      tasks_end_time->push_back(
          UpdateEndTimeForRunningTask(cur_td_ptr,
                                      *runtime_ptr,
                                      current_time_us,
                                      prev_num_tasks_colocated,
                                      cur_num_tasks_colocated));
    }
  }
}

}  // namespace sim
}  // namespace firmament
