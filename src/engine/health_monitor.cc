// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Health monitor for tasks and subordinate coordinators/resources. This runs
// in a thread spawned by the coordinator.

#include "engine/health_monitor.h"

#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/scheduler_interface.h"

namespace firmament {

HealthMonitor::HealthMonitor() {
}

void HealthMonitor::Run(SchedulerInterface* scheduler,
                        shared_ptr<ResourceMap_t> resources) {
  while (true) {
    sleep(10);
    VLOG(1) << "Health monitor checking on things...";
    scheduler->CheckRunningTasksHealth();
  }
}

}  // namespace firmament
