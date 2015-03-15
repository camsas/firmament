// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Health monitor for tasks and subordinate coordinators/resources. This runs
// in a thread spawned by the coordinator.

#ifndef FIRMAMENT_ENGINE_HEALTH_MONITOR_H
#define FIRMAMENT_ENGINE_HEALTH_MONITOR_H

#include <string>
#include <map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/scheduler_interface.h"

namespace firmament {

using scheduler::SchedulerInterface;

class HealthMonitor {
 public:
  HealthMonitor();
  void Run(SchedulerInterface* scheduler,
           shared_ptr<ResourceMap_t> resources);

 protected:
};

}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_HEALTH_MONITOR_H
