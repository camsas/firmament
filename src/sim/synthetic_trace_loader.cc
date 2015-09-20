// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/synthetic_trace_loader.h"

namespace firmament {
namespace sim {

SyntheticTraceLoader::SyntheticTraceLoader(EventManager* event_manager)
  : TraceLoader(event_manager) {
}

void SyntheticTraceLoader::LoadJobsNumTasks(
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  // TODO(ionel): Implement.
}

void SyntheticTraceLoader::LoadMachineEvents(
    uint64_t max_event_id_to_retain,
    multimap<uint64_t, EventDescriptor>* machine_events) {
  // TODO(ionel): Implement.
}

void SyntheticTraceLoader::LoadTaskEvents(uint64_t events_up_to_time) {
  // TODO(ionel): Implement.
}

void SyntheticTraceLoader::LoadTaskUtilizationStats(
    unordered_map<TraceTaskIdentifier, TraceTaskStats,
                  TraceTaskIdentifierHasher>* task_id_to_stats) {
  // TODO(ionel): Implement.
}

void SyntheticTraceLoader::LoadTasksRunningTime(
    uint64_t max_event_id_to_retain,
    unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
      task_runtime) {
  // TODO(ionel): Implement.
}

} // namespace sim
} // namespace firmament
