// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task graph representation, providing convenience methods for task
// graph transformations, such as adding tasks (as a result of spawn), or
// marking them as being in a certain state (completed, failed etc.).

#include <queue>
#include <set>

#include "base/task_graph.h"
#include "misc/map-util.h"
#include "misc/pb_utils-inl.h"

namespace firmament {

TaskGraph::TaskGraph(TaskDescriptor* root_task) {
  // Create a node for the root task
  root_node_ = new TaskGraphNode(root_task, NULL);
  InsertIfNotPresent(&td_node_map_, root_task, root_node_);
  VLOG(1) << root_node_->descriptor() << ", " << root_task;
  // Create nodes for all members of the task graph.
  CreateNodesForChildren(root_node_, root_task);
}

void TaskGraph::AddChildTask(TaskDescriptor* /*parent*/,
                             TaskDescriptor* /*child*/) {
  LOG(FATAL) << "stub";
}

void TaskGraph::CreateNodesForChildren(TaskGraphNode* node,
                                       TaskDescriptor* descr) {
  for (__typeof__(descr->mutable_spawned()->begin()) c_iter =
       descr->mutable_spawned()->begin();
       c_iter != descr->mutable_spawned()->end();
       ++c_iter) {
    TaskGraphNode* new_child = new TaskGraphNode(&(*c_iter), node);
    node->add_child(new_child);
    InsertIfNotPresent(&td_node_map_, &(*c_iter), new_child);
    CreateNodesForChildren(new_child, &(*c_iter));
  }
}

void TaskGraph::DelegateOutput(TaskDescriptor* delegator,
                               ReferenceDescriptor* output,
                               TaskDescriptor* delegatee) {
  // Check that output actually belongs to delegator
  if (!RepeatedContainsPtr(delegator->mutable_outputs(), output))
    LOG(ERROR) << "Failed to delegate output " << output->DebugString()
               << " of " << delegator->DebugString() << " to "
               << delegatee->DebugString() << " as it is not "
               << " actually produced by " << delegator->uid() << "!";
  // Add output to delegatee
  // - if we do not already have this reference as an output, add it
  ReferenceDescriptor* delegated_rd = delegatee->add_outputs();
  delegated_rd->CopyFrom(*output);
  // Set output state to delegated for original task
}

set<TaskDescriptor*> TaskGraph::ChildrenOf(TaskDescriptor* task) {
  set<TaskDescriptor*> children;
  for (RepeatedPtrField<TaskDescriptor>::iterator task_iter =
       task->mutable_spawned()->begin();
       task_iter != task->mutable_spawned()->end();
       ++task_iter) {
    children.insert(&(*task_iter));
  }
  return children;
}

TaskDescriptor* TaskGraph::ParentOf(TaskDescriptor* task) {
  TaskGraphNode** node = FindOrNull(td_node_map_, task);
  if (!node)
    return NULL;
  // If this is the root task, we return a pointer to itself
  if ((*node)->mutable_parent() == NULL)
    return task;
  // Otherwise return a pointer to the parent's TD
  return (*node)->mutable_parent()->descriptor();
}

void TaskGraph::SetTaskState(TaskDescriptor* /*task*/) {
  LOG(FATAL) << "stub";
}

bool TaskGraph::Contains(TaskDescriptor* task) {
  if (task == root_node_->descriptor())
    return true;
  // BFS of task graph to check if the task passed is part of this DTG.
  queue<TaskGraphNode*> td_queue;
  td_queue.push(root_node_);
  while (!td_queue.empty()) {
    TaskGraphNode* td_ptr = td_queue.front();
    CHECK_NOTNULL(td_ptr);
    td_queue.pop();
    if (td_ptr->descriptor() == task)
      return true;
    /*for (RepeatedPtrField<TaskDescriptor>::iterator task_iter =
         task->mutable_spawned()->begin();
         task_iter != task->mutable_spawned()->end();
         ++task_iter) {
      td_queue.push(&(*task_iter));
    }*/
  }
  return false;
}

}  // namespace firmament
