// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task representation.

#include "base/task.h"

namespace firmament {

Task::Task(const string& name) {
  // Set up the task descriptor.
  set_name(name);
  set_state(CREATED);

  // TODO(malte): Compute and set a UID for this task.
}

}  // namespace firmament
