// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common resource functionality.

#include "base/resource.h"

namespace firmament {

Resource::Resource(const string& name, uint32_t task_capacity) :
    current_ensemble_(NULL), busy_(false) {
  descriptor_.set_name(name);
  descriptor_.set_task_capacity(task_capacity);
}

bool Resource::RunTask(Task *task) {
  VLOG(1) << "Resource " << descriptor_.name() << " running task " << task->name();
  CHECK_NOTNULL(task);
//  set_busy(true);
  current_ensemble_->SetResourceBusy(this);
  return false;
}

void Resource::TaskExited() {
//  set_busy(false);
  current_ensemble_->SetResourceIdle(this);
}

bool Resource::JoinEnsemble(Ensemble *ensemble) {
  CHECK_NOTNULL(ensemble);
  ensemble->AddResource(*this);
  current_ensemble_ = ensemble;
  return true;
}

}  // namespace firmament
