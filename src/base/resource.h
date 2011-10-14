// TODO

#ifndef FIRMAMENT_BASE_RESOURCE_H
#define FIRMAMENT_BASE_RESOURCE_H

#include "base/common.h"
#include "base/task.h"
#include "base/ensemble.h"

namespace firmament {

class Ensemble;

class Resource {
 public:
  Resource(const string& name, uint32_t task_capacity);
  bool RunTask(Task *task);
  bool JoinEnsemble(Ensemble *ensemble);
  void TaskExited();

  double next_available() { return next_available_; }
  void set_next_available(double next_available) {
    next_available_ = next_available;
  }

  const string& name() { return name_; }
 private:
  string name_;
  uint64_t task_capacity_;
  Ensemble *current_ensemble_;
  Task *current_task_;
  double next_available_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_RESOURCE_H
