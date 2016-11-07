/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
