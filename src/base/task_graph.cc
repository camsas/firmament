// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task graph representation, providing convenience methods for task
// graph transformations, such as adding tasks (as a result of spawn), or
// marking them as being in a certain state (completed, failed etc.).

#include "base/task_graph.h"

namespace firmament {

TaskGraph::TaskGraph(TaskDescriptor* root_task) {
  // TODO(malte): set-up
}

void TaskGraph::AddChildTask(TaskDescriptor* parent, TaskDescriptor* child) {
  LOG(FATAL) << "stub";
}

void TaskGraph::DelegateOutput(TaskDescriptor* delegator,
                               ReferenceDescriptor* output,
                               TaskDescriptor* delegatee) {
  LOG(FATAL) << "stub";
}

void TaskGraph::SetTaskState(TaskDescriptor* task) {
  LOG(FATAL) << "stub";
}

}  // namespace firmament
