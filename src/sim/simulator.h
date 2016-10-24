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

// Trace simulator tool.

#ifndef FIRMAMENT_SIM_SIMULATOR_H
#define FIRMAMENT_SIM_SIMULATOR_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/event_manager.h"
#include "sim/simulated_wall_time.h"
#include "sim/simulator_bridge.h"
#include "sim/trace_utils.h"

DECLARE_string(flow_scheduling_binary);
DECLARE_bool(incremental_flow);
DECLARE_bool(only_read_assignment_changes);

namespace firmament {
namespace sim {

class Simulator {
 public:
  explicit Simulator();
  virtual ~Simulator();
  void Run();
  static void SchedulerTimeoutHandler(int sig);

 private:
  void ReplaySimulation();

  /**
   * Runs the scheduler.
   * @param the time when the scheduler should run
   * @return the time when the scheduler should run after the run at
   * run_scheduler_at
   */
  uint64_t ScheduleJobsHelper(uint64_t run_scheduler_at);

  SimulatorBridge* bridge_;
  EventManager* event_manager_;
  SimulatedWallTime simulated_time_;
  uint64_t scheduler_run_cnt_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATOR_H
