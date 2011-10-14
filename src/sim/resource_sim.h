// TODO: header

#ifndef FIRMAMENT_SIM_RESOURCE_SIM_H
#define FIRMAMENT_SIM_RESOURCE_SIM_H

#include "base/common.h"
#include "base/resource.h"

namespace firmament {

class ResourceSim : public Resource {
 public:
  ResourceSim(const string& name, uint32_t task_capacity )
      : Resource(name, task_capacity) {}
 private:
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_RESOURCE_SIM_H
