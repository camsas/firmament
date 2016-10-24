/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#ifndef FIRMAMENT_SIM_EVENT_MANAGER_H
#define FIRMAMENT_SIM_EVENT_MANAGER_H

#include <map>
#include <utility>

#include "base/common.h"
#include "misc/time_interface.h"
#include "sim/event_desc.pb.h"
#include "sim/simulated_wall_time.h"
#include "sim/trace_utils.h"

namespace firmament {
namespace sim {

class EventManager {
 public:
  explicit EventManager(SimulatedWallTime* simulated_time);
  virtual ~EventManager();

  /**
   * Adds a new event to the trace.
   * @param timestamp the time when the event happens
   * @param event struct describing the event
   */
  void AddEvent(uint64_t timestamp, EventDescriptor event);

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

 private:
  SimulatedWallTime* simulated_time_;
  // The map storing the simulator events. Maps from timestamp to simulator
  // event.
  multimap<uint64_t, EventDescriptor> events_;
  uint64_t num_events_processed_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENT_MANAGER_H
