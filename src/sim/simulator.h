// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
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
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATOR_H
