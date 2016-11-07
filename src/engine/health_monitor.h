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
