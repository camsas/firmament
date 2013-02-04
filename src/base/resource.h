// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common resource functionality and data structures.
// TODO(malte): Refactor this to become more shallow and introduce a separate
//              interface class.

#ifndef FIRMAMENT_BASE_RESOURCE_H
#define FIRMAMENT_BASE_RESOURCE_H

#include <string>

#include "base/common.h"
#include "base/task.h"
#include "base/ensemble.h"

#include "base/resource_desc.pb.h"

namespace firmament {

// Forward-declaration to break cyclic dependency.
class Ensemble;

class Resource {
 public:
  Resource(const string& descriptive_name, uint32_t task_capacity);
  bool JoinEnsemble(Ensemble *ensemble);
  bool RunTask(Task *task);
  void TaskExited();

  const string& name() { return descriptor_.descriptive_name(); }
  bool busy() const { return busy_; }
  void set_busy(bool b) { busy_ = b; }
 protected:
  ResourceDescriptor descriptor_;
 private:
  Ensemble *current_ensemble_;
  bool busy_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_RESOURCE_H
