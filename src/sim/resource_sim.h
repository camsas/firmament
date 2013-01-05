// TODO: header

#ifndef FIRMAMENT_SIM_RESOURCE_SIM_H
#define FIRMAMENT_SIM_RESOURCE_SIM_H

#include "base/common.h"
#include "base/resource.h"

namespace firmament {

class ResourceSim : public Resource {
 public:
  ResourceSim(const string& name, uint32_t task_capacity);
  bool RunTask(Task *task);

  double next_available() { return next_available_; }
  void set_next_available(double next_available) {
    next_available_ = next_available;
  }

 private:
  double next_available_;
  //Task *current_task_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_RESOURCE_SIM_H
