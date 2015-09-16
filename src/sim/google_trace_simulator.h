// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace simulator tool.

#ifndef FIRMAMENT_SIM_GOOGLE_TRACE_SIMULATOR_H
#define FIRMAMENT_SIM_GOOGLE_TRACE_SIMULATOR_H

#include <map>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/solver_dispatcher.h"
#include "sim/event_manager.h"
#include "sim/google_trace_bridge.h"
#include "sim/trace_utils.h"

DECLARE_string(flow_scheduling_binary);
DECLARE_bool(incremental_flow);
DECLARE_bool(only_read_assignment_changes);
DECLARE_bool(add_root_task_to_graph);
DECLARE_bool(flow_scheduling_strict);
DECLARE_bool(flow_scheduling_time_reported);

namespace firmament {
namespace sim {

class GoogleTraceSimulator {
 public:
  explicit GoogleTraceSimulator(const string& trace_path);
  virtual ~GoogleTraceSimulator();
  void Run();
  static void SolverTimeoutHandler(int sig);

 private:
  /**
   * Processes all the simulator events that happen at a given time.
   * @param cur_time the timestamp for which to process the simulator events
   * @param machine_tmpl topology to use in case new machines are added
   */
  void ProcessSimulatorEvents(
      uint64_t events_up_to_time,
      const ResourceTopologyNodeDescriptor& machine_tmpl);

  void ReplayTrace();

  /**
   * Reset the fields used to maintain statistics about the current scheduling
   * latency. This method should be called after every run of the scheduler.
   */
  void ResetSchedulingLatencyStats();

  /**
   * Runs the solver.
   * @param the time when the solver should run
   * @return the time when the solver should run after the run at run_solver_at
   */
  uint64_t RunSolverHelper(uint64_t run_solver_at);

  /**
   * Update the fields used to maintain statistics about the current scheduling
   * latency. This method should be called whenever a trace event happens.
   */
  void UpdateSchedulingLatencyStats(uint64_t time);

  GoogleTraceBridge* bridge_;
  EventManager* event_manager_;

  // Timestamp of the first event seen in the current scheduling round. Any
  // event present in the original trace is record, as are those which we have
  // created to replace events in the trace, e.g. when rerunning task runtime.
  // If no event has been seen then the value of the variable is UINT64_MAX.
  uint64_t first_event_in_scheduling_round_;
  uint64_t last_event_in_scheduling_round_;
  uint64_t num_events_in_scheduling_round_;
  uint64_t sum_timestamps_in_scheduling_round_;

  // Counter used to store the number of duplicate task ids seed in the trace.
  uint64_t num_duplicate_task_ids_;

  string trace_path_;

  // File to output graph to. (Optional; NULL if unspecified.)
  FILE *graph_output_;
  // File to output stats to. (Optional; NULL if unspecified.)
  FILE *stats_file_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_GOOGLE_TRACE_SIMULATOR_H
