// The Firmament project
// Copyright (c) 2011-2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Health monitor for tasks and subordinate coordinators/resources. This runs
// in a thread spawned by the coordinator.

#include "engine/health_monitor.h"

#include <unistd.h>
#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/scheduler_interface.h"

DEFINE_bool(health_monitor_enable, true, "Enabled checks for failed tasks.");
DEFINE_int32(health_monitor_check_frequency, 10000000ULL,
             "Frequency at which the task health monitor checks on tasks' "
             "liveness, in microseconds.");

namespace firmament {

HealthMonitor::HealthMonitor() {
}

void HealthMonitor::Run(SchedulerInterface* scheduler,
                        shared_ptr<ResourceMap_t> resources) {
  while (FLAGS_health_monitor_enable) {
    usleep(FLAGS_health_monitor_check_frequency);
    VLOG(1) << "Health monitor checking on things...";
    scheduler->CheckRunningTasksHealth();
  }
}

}  // namespace firmament
