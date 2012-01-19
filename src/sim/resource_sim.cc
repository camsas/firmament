// TODO: header

#include "sim/resource_sim.h"

namespace firmament {

ResourceSim::ResourceSim(const string& name, uint32_t task_capacity) :
    Resource(name, task_capacity), next_available_(-1) {
}

bool ResourceSim::RunTask(Task *task) {
  current_task_ = task;
  return Resource::RunTask(task);
}

void ResourceSim::TaskExited() {
  // TODO(malte): This needs abolishing, or a complete rewrite, as it makes the
  // inappropriate assumption that a resource only runs one task at a time.
  VLOG(1) << "current_task_ is " << current_task_ << " on resource " << name_;
  CHECK_NOTNULL(current_task_);
  current_task_ = NULL;
}

}  // namespace firmament
