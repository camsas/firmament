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
DECLARE_bool(add_root_task_to_graph);
DECLARE_bool(flow_scheduling_strict);
DECLARE_bool(flow_scheduling_time_reported);

namespace firmament {
namespace sim {

class Simulator {
 public:
  explicit Simulator();
  virtual ~Simulator();
  void Run();
  static void SchedulerTimeoutHandler(int sig);

 private:
  /**
   * Loads the trace task events that happened before or at events_up_to_time.
   * NOTE: this method might end up loading an event that happened after
   * events_up_to_time. However, this has no effect on the correctness of the
   * simulator.
   * @param events_up_to_time the time up to which to load the events
   */
  void LoadTraceTaskEvents(uint64_t events_up_to_time);

  /**
   * Processes all the simulator events that happen at a given time.
   * @param cur_time the timestamp for which to process the simulator events
   * @param machine_tmpl topology to use in case new machines are added
   */
  void ProcessSimulatorEvents(
      uint64_t events_up_to_time,
      const ResourceTopologyNodeDescriptor& machine_tmpl);

  void ReplaySimulation();

  /**
   * Reset the fields used to maintain statistics about the current scheduling
   * latency. This method should be called after every run of the scheduler.
   */
  void ResetSchedulingLatencyStats();

  /**
   * Runs the scheduler.
   * @param the time when the scheduler should run
   * @return the time when the scheduler should run after the run at
   * run_scheduler_at
   */
  uint64_t ScheduleJobsHelper(uint64_t run_scheduler_at);

  /**
   * Update the fields used to maintain statistics about the current scheduling
   * latency. This method should be called whenever a trace event happens.
   */
  void UpdateSchedulingLatencyStats(uint64_t time);

  SimulatorBridge* bridge_;
  EventManager* event_manager_;

  // Timestamp of the first event seen in the current scheduling round. Any
  // event present in the original trace is record, as are those which we have
  // created to replace events in the trace, e.g. when rerunning task runtime.
  // If no event has been seen then the value of the variable is UINT64_MAX.
  uint64_t first_event_in_scheduling_round_;
  uint64_t last_event_in_scheduling_round_;
  uint64_t num_events_in_scheduling_round_;
  uint64_t sum_timestamps_in_scheduling_round_;

  // File from which to read the task events.
  FILE* task_events_file_;
  // The number of the task events file the simulator is reading from.
  int32_t current_task_events_file_;

  // Counter used to store the number of duplicate task ids seed in the trace.
  uint64_t num_duplicate_task_ids_;
  // File to output graph to. (Optional; NULL if unspecified.)
  FILE *graph_output_;
  // File to output stats to. (Optional; NULL if unspecified.)
  FILE *stats_file_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATOR_H
