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
  void LoadMachineEvents(multimap<uint64_t, EventDescriptor>* machine_events);

  /**
   * Loads the trace task events that happened before or at events_up_to_time.
   * NOTE: this method might end up loading an event that happened after
   * events_up_to_time. However, this has no effect on the correctness of the
   * simulator.
   * @param events_up_to_time the time up to which to load the events
   * @param job_num_tasks map containing the number of tasks each job has. The
   * map is going to be updated if any task events are filtered.
   */
  void LoadTaskEvents(uint64_t events_up_to_time,
                      unordered_map<uint64_t, uint64_t>* job_num_tasks);

  void LoadTaskUtilizationStats(
      unordered_map<TraceTaskIdentifier, TraceTaskStats,
        TraceTaskIdentifierHasher>* task_id_to_stats);

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  void LoadTasksRunningTime(
      unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
        task_runtime);

 private:
  uint64_t MaxEventHashToRetain();

  // The number of the task events file the simulator is reading from.
  int32_t current_task_events_file_id_;
  // File from which to read the task events.
  FILE* task_events_file_;
  // The first time we encounter a filtered task we must update the number of
  // tasks its corresponding job has. However, upon subsequent encounters we do
  // not have to do that. We use this collection to maintain a set of tasks
  // that have already been filtered.
  unordered_set<TraceTaskIdentifier, TraceTaskIdentifierHasher> filtered_tasks_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_GOOGLE_TRACE_LOADER_H
