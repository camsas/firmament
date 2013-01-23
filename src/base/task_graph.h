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

class TaskGraphNode {
 public:
  TaskGraphNode(TaskDescriptor* desc, TaskGraphNode* spawner)
      : desc_(desc), parent_(spawner) {
    VLOG(2) << "Creating new TaskGraphNode, root task descriptor is at "
            << desc_ << "(" << desc << ")";
  }
  const TaskGraphNode& parent() const {
    return *parent_;
  }
  TaskGraphNode* mutable_parent() const {
    return parent_;
  }
  TaskDescriptor* descriptor() const {
    return desc_;
  }
  const vector<TaskGraphNode*>& children() const {
    return children_;
  }
  const TaskGraphNode& child(uint64_t idx) {
    CHECK_LT(idx, children_.size());
    return *children_.at(idx);
  }
  TaskGraphNode* mutable_child(uint64_t idx) {
    CHECK_LT(idx, children_.size());
    return children_.at(idx);
  }
 protected:
  vector<TaskGraphNode*> children_;
  TaskDescriptor* desc_;
  TaskGraphNode* parent_;
};

class TaskGraph {
 public:
  explicit TaskGraph(TaskDescriptor* root_task_);
  void AddChildTask(TaskDescriptor* parent, TaskDescriptor* child);
  set<TaskDescriptor*> ChildrenOf(TaskDescriptor* task);
  void DelegateOutput(TaskDescriptor* delegator, ReferenceDescriptor* output,
                      TaskDescriptor* delegatee);
  TaskDescriptor* ParentOf(TaskDescriptor* task);
  void SetTaskState(TaskDescriptor* task);
 protected:
  FRIEND_TEST(TaskGraphTest, CreateTGTest);
  FRIEND_TEST(TaskGraphTest, GetParentTest);
  TaskGraphNode* root_node_;
  bool Contains(TaskDescriptor* task);
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_GRAPH_H
