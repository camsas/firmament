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

// The executor interface assumed by the engine.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H
#define FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H

#include "misc/printable_interface.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/task_final_report.pb.h"

namespace firmament {
namespace executor {

class ExecutorInterface : public PrintableInterface {
 public:
  virtual bool CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks) = 0;
  virtual void HandleTaskCompletion(TaskDescriptor* td,
                                    TaskFinalReport* report) = 0;
  virtual void HandleTaskEviction(TaskDescriptor* td) = 0;
  virtual void HandleTaskFailure(TaskDescriptor* td) = 0;
  virtual void RunTask(TaskDescriptor* td,
                       bool firmament_binary) = 0;
  virtual ostream& ToString(ostream* stream) const = 0;

 protected:
};

}  // namespace executor
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_EXECUTOR_INTERFACE_H
