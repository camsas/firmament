// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_EVENT_MANAGER_H
#define FIRMAMENT_SIM_EVENT_MANAGER_H

#include <map>
#include <utility>

#include "base/common.h"
#include "misc/time_interface.h"
#include "sim/event_desc.pb.h"
#include "sim/trace_utils.h"

namespace firmament {
namespace sim {

class EventManager : public TimeInterface {
 public:
  EventManager();
  virtual ~EventManager();

  /**
   * Adds a new event to the trace.
   * @param timestamp the time when the event happens
   * @param event struct describing the event
   */
  void AddEvent(uint64_t timestamp, EventDescriptor event);

  /**
   * Get the current timestamp of the simulation (in u-sec).
   */
  uint64_t GetCurrentTimestamp();

  /**
   * Get the next simulated event.
   * @return a pair consisting of a timestamp and an event descriptor
   */
  pair<uint64_t, EventDescriptor> GetNextEvent();

  /**
   * Time of the next simulator event. UINT64_MAX if no more simulator events.
   */
  uint64_t GetTimeOfNextEvent();

  /**
   * Returns the time when the scheduler should be executed next.
   * @param cur_run_scheduler_at the time of the last scheduler run
   * @param scheduler_runtime the duration of the last scheduler run
   */
  uint64_t GetTimeOfNextSchedulerRun(uint64_t cur_run_scheduler_at,
                                     uint64_t scheduler_runtime);

  /**
   * Returns true if the simulation should stop.
   * @param num_scheduling the number of scheduler runs
   * @param true if the simulation should stop
   */
  bool HasSimulationCompleted(uint64_t num_scheduling_rounds);

  /**
   * Removes the task's end event from the simulator's event queue.
   * @param task_identifier the trace identifier of the task for which to
   * remove the event
   * @param task_end_time the time of the event
   */
  void RemoveTaskEndRuntimeEvent(const TraceTaskIdentifier& task_identifier,
                                 uint64_t task_end_time);

  /**
   * Overwrites the current timestmap of the simulation.
   */
  void UpdateCurrentTimestamp(uint64_t timestamp);

  uint64_t current_simulation_time() {
    return current_simulation_time_;
  }

 private:
  uint64_t current_simulation_time_;
  // The map storing the simulator events. Maps from timestamp to simulator
  // event.
  multimap<uint64_t, EventDescriptor> events_;
  uint64_t num_events_processed_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENT_MANAGER_H
