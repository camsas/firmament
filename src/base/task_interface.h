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

// Common task representation.

#ifndef FIRMAMENT_BASE_TASK_INTERFACE_H
#define FIRMAMENT_BASE_TASK_INTERFACE_H

#include <string>
#include <vector>

#include "base/common.h"
#include "base/data_object.h"
#include "base/types.h"
#include "engine/task_lib.h"
#include "misc/printable_interface.h"

namespace firmament {

// Main task invocation method. This will be linked to the
// implementation-specific task_main() procedure in the task implementation.
// TODO(malte): Ideally, we wouldn't need this level of indirection.
extern void task_main(TaskID_t task_id, vector<char *>* arg_vec);

class TaskInterface : public PrintableInterface {
 public:
  explicit TaskInterface(TaskID_t task_id)
    : id_(task_id) {}

  // Top-level task run invocation.
  //virtual void Invoke() = 0;

  // Print-friendly representation
  virtual ostream& ToString(ostream* stream) const {
    return *stream << "<Task, id=" << id_ << ">";
  }

 protected:
  // The task's unique identifier. Note that any TaskID_t is by definition
  // const, i.e. immutable.
  TaskID_t id_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_INTERFACE_H
