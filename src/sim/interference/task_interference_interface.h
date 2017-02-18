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

#ifndef FIRMAMENT_SIM_INTERFERENCE_TASK_INTERFERENCE_INTERFACE_H
#define FIRMAMENT_SIM_INTERFERENCE_TASK_INTERFERENCE_INTERFACE_H

#include "base/types.h"

namespace firmament {
namespace sim {

class TaskEndRuntimes {
 public:
  TaskEndRuntimes(TaskID_t task_id) :
    task_id_(task_id),
    has_previous_end_time_(false),
    has_current_end_time_(false) {}

  void set_previous_end_time(uint64_t previous_end_time) {
    previous_end_time_ = previous_end_time;
    has_previous_end_time_ = true;
  }
  void set_current_end_time(uint64_t current_end_time) {
    current_end_time_ = current_end_time;
    has_current_end_time_ = true;
  }

  uint64_t get_previous_end_time() const {
    CHECK(has_previous_end_time_);
    return previous_end_time_;
  }

  uint64_t get_current_end_time() const {
    CHECK(has_current_end_time_);
    return current_end_time_;
  }

  bool has_previous_end_time() const {
    return has_previous_end_time_;
  }

  bool has_current_end_time() const {
    return has_current_end_time_;
  }

  TaskID_t task_id_;

 private:
  uint64_t previous_end_time_;
  uint64_t current_end_time_;
  bool has_previous_end_time_;
  bool has_current_end_time_;
};

class TaskInterferenceInterface {
 public:
  TaskInterferenceInterface() {}
  virtual ~TaskInterferenceInterface() {}

  virtual void OnTaskCompletion(
      uint64_t current_time_us,
      TaskDescriptor* td_ptr,
      ResourceID_t res_id,
      vector<TaskEndRuntimes>* colocated_tasks_end_time) = 0;
  virtual void OnTaskEviction(
      uint64_t current_time_us,
      TaskDescriptor* td_ptr,
      ResourceID_t res_id,
      vector<TaskEndRuntimes>* colocated_tasks_end_time) = 0;
  virtual void OnTaskMigration(
      uint64_t current_time_us,
      TaskDescriptor* td_ptr,
      ResourceID_t old_res_id,
      ResourceID_t res_id,
      vector<TaskEndRuntimes>* colocated_tasks_end_time) = 0;
  virtual void OnTaskPlacement(
      uint64_t current_time_us,
      TaskDescriptor* td_ptr,
      ResourceID_t res_id,
      vector<TaskEndRuntimes>* colocated_tasks_end_time) = 0;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_INTERFERENCE_TASK_INTERFERENCE_INTERFACE_H
