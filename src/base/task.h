// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task representation.
//
// To avoid platform-specific task types (which may cause havoc in the future),
// we make the logic to actually run a task part ofthe resource implementation
// and keep Task entirely cross-platform.

#ifndef FIRMAMENT_BASE_TASK_H
#define FIRMAMENT_BASE_TASK_H

#include "base/common.h"
#include "base/job.h"

#include "generated/base/task_desc.pb.h"

namespace firmament {

class Task {
 public:
  Task(const string& name);
  uint64_t uid() { return descriptor_.uid(); }
  uint64_t index_in_job() { return descriptor_.index(); }
  void set_index_in_job(const uint64_t idx) { descriptor_.set_index(idx); }
  const string& name() { return descriptor_.name(); }
  void set_name(const string& name) { descriptor_.set_name(name); }
  TaskState state() { return static_cast<TaskState>(descriptor_.state()); }
  void set_state(TaskState state) {
    descriptor_.set_state(static_cast<TaskState>(state));
  }

 protected:
  TaskDescriptor descriptor_;
 private:
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_H
