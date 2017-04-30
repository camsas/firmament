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

#ifndef FIRMAMENT_SIM_INTERFERENCE_NO_TASK_INTERFERENCE_H
#define FIRMAMENT_SIM_INTERFERENCE_NO_TASK_INTERFERENCE_H

#include "sim/interference/task_interference_interface.h"

namespace firmament {
namespace sim {

class NoTaskInterference : public TaskInterferenceInterface {
 public:
  NoTaskInterference(unordered_map<TaskID_t, uint64_t>* task_runtime);
  ~NoTaskInterference();

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
  // Map holding the per-task runtime information
  unordered_map<TaskID_t, uint64_t>* task_runtime_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_INTERFERENCE_NO_TASK_INTERFERENCE_H
