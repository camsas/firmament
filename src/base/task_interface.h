// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task representation.

#ifndef FIRMAMENT_BASE_TASK_INTERFACE_H
#define FIRMAMENT_BASE_TASK_INTERFACE_H

#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "engine/task_lib.h"
#include "misc/printable_interface.h"

namespace firmament {

// Forward declaration.
class TaskLib;

// Main task invocation method. This will be linked to the
// implementation-specific task_main() procedure in the task implementation.
// TODO(malte): Ideally, we wouldn't need this level of indirection.
extern void task_main(TaskLib* task_lib, TaskID_t task_id,
                      vector<char *>* arg_vec);

class TaskInterface : public PrintableInterface {
 public:
  explicit TaskInterface(TaskLib* task_lib, TaskID_t task_id)
    : id_(task_id),
      task_lib_(task_lib) {}
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
  // Pointer to the associated task library, providing functionality such as
  // task spawn and object store access.
  TaskLib* task_lib_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_INTERFACE_H
