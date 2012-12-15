// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task graph representation, providing convenience methods for task
// graph transformations, such as adding tasks (as a result of spawn), or
// marking them as being in a certain state (completed, failed etc.).

#ifndef FIRMAMENT_BASE_TASK_GRAPH_H
#define FIRMAMENT_BASE_TASK_GRAPH_H

#include <string>

#include "base/common.h"
#include "base/task_desc.pb.h"

namespace firmament {

class TaskGraph {
 public:
  explicit TaskGraph(TaskDescriptor* root_task_);
  void AddChildTask(TaskDescriptor* parent, TaskDescriptor* child);
  void DelegateOutput(TaskDescriptor* delegator, ReferenceDescriptor* output,
                      TaskDescriptor* delegatee);
  void SetTaskState(TaskDescriptor* task);
 protected:
  TaskDescriptor* root_task_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_GRAPH_H
