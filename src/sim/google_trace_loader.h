// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace loader.

#ifndef FIRMAMENT_SIM_GOOGLE_TRACE_LOADER_H
#define FIRMAMENT_SIM_GOOGLE_TRACE_LOADER_H

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/map-util.h"
#include "sim/event_desc.pb.h"
#include "sim/event_manager.h"
#include "sim/trace_loader.h"
#include "sim/trace_utils.h"

namespace firmament {
namespace sim {

class GoogleTraceLoader : public TraceLoader {
 public:
  explicit GoogleTraceLoader(EventManager* event_manager);
  ~GoogleTraceLoader();

  void LoadJobsNumTasks(unordered_map<uint64_t, uint64_t>* job_num_tasks);

  /**
   * Loads all the machine events and returns a multimap timestamp -> event.
   */
  void LoadMachineEvents(
      uint64_t max_event_id_to_retain,
      multimap<uint64_t, EventDescriptor>* machine_events);

  /**
   * Loads the trace task events that happened before or at events_up_to_time.
   * NOTE: this method might end up loading an event that happened after
   * events_up_to_time. However, this has no effect on the correctness of the
   * simulator.
   * @param events_up_to_time the time up to which to load the events
   */
  void LoadTaskEvents(uint64_t events_up_to_time);

  void LoadTaskUtilizationStats(
      unordered_map<TraceTaskIdentifier, TraceTaskStats,
        TraceTaskIdentifierHasher>* task_id_to_stats);

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  void LoadTasksRunningTime(
      uint64_t max_event_id_to_retain,
      unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
        task_runtime);

 private:
  // The number of the task events file the simulator is reading from.
  int32_t current_task_events_file_;
  // File from which to read the task events.
  FILE* task_events_file_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_GOOGLE_TRACE_LOADER_H
