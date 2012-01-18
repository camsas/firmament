// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common task representation.

#include "base/task.h"

namespace firmament {

Task::Task(const string& name)
  : name_(name),
    task_uid_(0),
    state_(Task::CREATED) {
}

}  // namespace firmament
