// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common resource functionality.

#include "base/resource.h"

namespace firmament {

Resource::Resource(const string& name, uint32_t task_capacity) :
    name_(name), task_capacity_(task_capacity), current_task_(NULL),
    next_available_(0), current_ensemble_(NULL), busy_(false) {
}

bool Resource::RunTask(Task *task) {
  VLOG(1) << "Resource " << name_ << " running task " << task->name();
  CHECK_NOTNULL(task);
//  set_busy(true);
  current_ensemble_->SetResourceBusy(this);
  current_task_ = task;
  return false;
}

void Resource::TaskExited() {
  VLOG(1) << "current_task_ is " << current_task_ << " on resource " << name_;
  CHECK_NOTNULL(current_task_);
//  set_busy(false);
  current_ensemble_->SetResourceIdle(this);
  current_task_ = NULL;
}

bool Resource::JoinEnsemble(Ensemble *ensemble) {
  CHECK_NOTNULL(ensemble);
  ensemble->AddResource(*this);
  current_ensemble_ = ensemble;
  return true;
}

}  // namespace firmament
