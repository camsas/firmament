// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Google cluster trace loader.

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_LOADER_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_LOADER_H

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/map-util.h"
#include "sim/trace-extract/event_desc.pb.h"
#include "sim/trace-extract/google_trace_utils.h"

DECLARE_uint64(runtime);

namespace firmament {
namespace sim {

class GoogleTraceLoader {
 public:
  GoogleTraceLoader(const string& trace_path);

  void LoadJobsNumTasks(unordered_map<uint64_t, uint64_t>* job_num_tasks);

  /**
   * Loads all the machine events and returns a multimap timestamp -> event.
   */
  void LoadMachineEvents(
      uint64_t max_event_id_to_retain,
      multimap<uint64_t, EventDescriptor>* machine_events);

  void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl);

  void LoadTaskUtilizationStats(
      unordered_map<TraceTaskIdentifier, TaskStats,
        TraceTaskIdentifierHasher>* task_id_to_stats);

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  void LoadTasksRunningTime(
      uint64_t max_event_id_to_retain,
      unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
        task_runtime);

 private:
  string trace_path_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_LOADER_H
