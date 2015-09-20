// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Synthetic trace loader.

#ifndef FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H
#define FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H

#include "sim/event_manager.h"
#include "sim/trace_loader.h"

namespace firmament {
namespace sim {

class SyntheticTraceLoader : public TraceLoader {
 public:
  explicit SyntheticTraceLoader(EventManager* event_manager);
  void LoadJobsNumTasks(unordered_map<uint64_t, uint64_t>* job_num_tasks);
  void LoadMachineEvents(
      uint64_t max_event_id_to_retain,
      multimap<uint64_t, EventDescriptor>* machine_events);
  void LoadTaskEvents(uint64_t events_up_to_time);
  void LoadTaskUtilizationStats(
      unordered_map<TraceTaskIdentifier, TraceTaskStats,
        TraceTaskIdentifierHasher>* task_id_to_stats);
  void LoadTasksRunningTime(
      uint64_t max_event_id_to_retain,
      unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
        task_runtime);
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SYNTHETIC_TRACE_LOADER_H
