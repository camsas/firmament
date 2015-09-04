// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EVENT_MANAGER_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EVENT_MANAGER_H

#include <map>
#include <utility>

#include "base/common.h"
#include "sim/trace-extract/event_desc.pb.h"
#include "sim/trace-extract/google_trace_utils.h"

namespace firmament {
namespace sim {

class GoogleTraceEventManager {
 public:
  GoogleTraceEventManager();

  void AddEvent(uint64_t timestamp, EventDescriptor event);

  uint64_t current_simulation_time() {
    return current_simulation_time_;
  }

  pair<uint64_t, EventDescriptor> GetNextEvent();

  /**
   * Time of the next simulator event. UINT64_MAX if no more simulator events.
   */
  uint64_t GetTimeOfNextEvent();

  uint64_t GetTimeOfNextSolverRun(uint64_t cur_run_solver_at,
                                  double scheduler_runtime);

  bool HasSimulationCompleted(uint64_t num_events,
                              uint64_t num_scheduling_rounds);

  void RemoveTaskEndRuntimeEvent(const TraceTaskIdentifier& task_identifier,
                                 uint64_t task_end_time);

 private:
  uint64_t current_simulation_time_ = 0;
  // The map storing the simulator events. Maps from timestamp to simulator
  // event.
  multimap<uint64_t, EventDescriptor> events_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_EVENT_MANAGER_H
