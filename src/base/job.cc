// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common job functionality.

#include "base/job.h"

namespace firmament {

Job::Job(const string& name)
  : name_(name),
    state_(Job::UNKNOWN),
    num_tasks_running_(0) {
}

bool Job::AddTask(Task *const t) {
  tasks_.push_back(t);
  return true;
}

}  // namespace firmament
