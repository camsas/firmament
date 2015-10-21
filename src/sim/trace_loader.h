// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Trace loader interface.

#ifndef FIRMAMENT_SIM_TRACE_LOADER_H
#define FIRMAMENT_SIM_TRACE_LOADER_H

#include "base/common.h"
#include "sim/event_manager.h"
#include "sim/trace_utils.h"

namespace firmament {
namespace sim {

class TraceLoader {
 public:
  TraceLoader(EventManager* event_manager) : event_manager_(event_manager) {
  }

  virtual ~TraceLoader() {
    // We don't delete event_manager_ because it is owned by the simulator.
  }

  virtual void LoadJobsNumTasks(
      unordered_map<uint64_t, uint64_t>* job_num_tasks) = 0;

  /**
   * Loads all the machine events and returns a multimap timestamp -> event.
   */
  virtual void LoadMachineEvents(
      multimap<uint64_t, EventDescriptor>* machine_events) = 0;

  /**
   * Loads the trace task events that happened before or at events_up_to_time.
   * @param events_up_to_time the time up to which to load the events
   * @param job_num_tasks map containing the number of tasks each job has. The
   * map is going to be updated if any task events are filtered.
   */
  virtual void LoadTaskEvents(
      uint64_t events_up_to_time,
      unordered_map<uint64_t, uint64_t>* job_num_tasks) = 0;

  virtual void LoadTaskUtilizationStats(
      unordered_map<TraceTaskIdentifier, TraceTaskStats,
        TraceTaskIdentifierHasher>* task_id_to_stats) = 0;

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  virtual void LoadTasksRunningTime(
      unordered_map<TraceTaskIdentifier, uint64_t, TraceTaskIdentifierHasher>*
        task_runtime) = 0;

 protected:
  EventManager* event_manager_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_LOADER_H
