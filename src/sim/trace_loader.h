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
   * @return false if no events have been loaded and there are no more events
   * left to be loaded.
   */
  virtual bool LoadTaskEvents(
      uint64_t events_up_to_time,
      unordered_map<uint64_t, uint64_t>* job_num_tasks) = 0;

  virtual void LoadTaskUtilizationStats(
      unordered_map<TaskID_t, TraceTaskStats>* task_id_to_stats,
      const unordered_map<TaskID_t, uint64_t>& task_runtimes) = 0;

  /**
   * Loads all the task runtimes and returns map task_identifier -> runtime.
   */
  virtual void LoadTasksRunningTime(
      unordered_map<TaskID_t, uint64_t>* task_runtime) = 0;

 protected:
  EventManager* event_manager_;
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_LOADER_H
