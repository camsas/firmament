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

#ifndef FIRMAMENT_SIM_INTERFERENCE_QUINCY_TASK_INTERFERENCE_H
#define FIRMAMENT_SIM_INTERFERENCE_QUINCY_TASK_INTERFERENCE_H

#include "sim/interference/task_interference_interface.h"

#include "scheduling/scheduler_interface.h"

namespace firmament {
namespace sim {

class QuincyTaskInterference : public TaskInterferenceInterface {
 public:
  QuincyTaskInterference(
      scheduler::SchedulerInterface* scheduler,
      multimap<ResourceID_t, ResourceDescriptor*>* machine_res_id_pus,
      shared_ptr<ResourceMap_t> resource_map,
      shared_ptr<TaskMap_t> task_map,
      unordered_map<TaskID_t, uint64_t>* task_runtime);
  ~QuincyTaskInterference();

  void OnTaskCompletion(uint64_t current_time_us,
                        TaskDescriptor* td_ptr,
                        ResourceID_t res_id,
                        vector<TaskEndRuntimes>* tasks_end_time);
  void OnTaskEviction(uint64_t current_time_us,
                      TaskDescriptor* td_ptr,
                      ResourceID_t res_id,
                      vector<TaskEndRuntimes>* tasks_end_time);
  void OnTaskMigration(uint64_t current_time_us,
                       TaskDescriptor* td_ptr,
                       ResourceID_t old_res_id,
                       ResourceID_t res_id,
                       vector<TaskEndRuntimes>* tasks_end_time);
  void OnTaskPlacement(uint64_t current_time_us,
                       TaskDescriptor* td_ptr,
                       ResourceID_t res_id,
                       vector<TaskEndRuntimes>* tasks_end_time);

 private:
  /**
   * Get the tasks that are co-located on the PU and on the machine.
   * @param the id of the PU resource
   * @param vector to store tasks co-located on the PU
   * @param vector to store tasks co-located on the machine, but not on the PU
   */
  void GetColocatedTasks(ResourceID_t pu_res_id,
                         vector<TaskID_t>* colocated_on_pu,
                         vector<TaskID_t>* colocated_on_machine);

  uint64_t TimeWithInterferenceToTraceTime(uint64_t time,
                                           uint64_t num_tasks_colocated);
  uint64_t TraceTimeToTimeWithInterference(uint64_t time,
                                           uint64_t num_tasks_colocated);
  TaskEndRuntimes UpdateEndTimeForPlacedTask(TaskDescriptor* td_ptr,
                                             uint64_t current_time_us,
                                             uint64_t num_tasks_colocated);
  TaskEndRuntimes UpdateEndTimeForRunningTask(TaskDescriptor* td_ptr,
                                              uint64_t previous_end_time,
                                              uint64_t current_time_us,
                                              uint64_t prev_num_tasks_colocated,
                                              uint64_t cur_num_tasks_colocated);
  void UpdateOtherTasksOnMachine(uint64_t current_time_us,
                                 uint64_t prev_num_tasks_colocated,
                                 uint64_t cur_num_tasks_colocated,
                                 const vector<TaskID_t>& colocated_on_pu,
                                 const vector<TaskID_t>& colocated_on_machine,
                                 vector<TaskEndRuntimes>* tasks_end_time);

  scheduler::SchedulerInterface* scheduler_;
  // Multimap storing the mapping between machine resource ids and their PU
  // resource descriptors.
  multimap<ResourceID_t, ResourceDescriptor*>* machine_res_id_pus_;
  // Map from ResourceID_t to ResourceStatus*
  shared_ptr<ResourceMap_t> resource_map_;
  // Map from TaskID_t to TaskDescriptor*
  shared_ptr<TaskMap_t> task_map_;
  // Map holding the per-task runtime information
  unordered_map<TaskID_t, uint64_t>* task_runtime_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_INTERFERENCE_QUINCY_TASK_INTERFERENCE_H
