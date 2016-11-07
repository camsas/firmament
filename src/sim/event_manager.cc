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

#include "sim/event_manager.h"

#include <algorithm>
#include <utility>

#include "base/units.h"
#include "misc/utils.h"

DEFINE_uint64(batch_step, 0, "Batch mode: time interval to run scheduler "
              "at (in microseconds).");
DEFINE_uint64(max_events, UINT64_MAX,
              "Maximum number of task events to process.");
DEFINE_uint64(max_scheduling_rounds, UINT64_MAX,
              "Maximum number of scheduling rounds to run for.");
DEFINE_double(online_factor, 0.0, "Online mode: speed at which to run at. "
              "Factor of 1 corresponds to real-time. Larger to include"
              " overheads elsewhere in Firmament etc., smaller to simulate"
              " solver running faster.");
DEFINE_uint64(runtime, UINT64_MAX,
              "Maximum time in microsec to extract data for"
              "(from start of trace)");

DECLARE_double(trace_speed_up);

static bool ValidateBatchStep(const char* flagname, uint64_t batch_step) {
  if (batch_step == 0) {
    if (firmament::IsEqual(FLAGS_online_factor, 0.0)) {
      LOG(ERROR) << "must specify one of -batch_step or -online_factor";
      return false;
    }
    return true;
  } else {
    if (!firmament::IsEqual(FLAGS_online_factor, 0.0)) {
      LOG(ERROR) << "cannot specify both -batch_step and -online_factor";
      return false;
    }
    return true;
  }
}

static const bool batch_step_validator =
  google::RegisterFlagValidator(&FLAGS_batch_step, &ValidateBatchStep);

namespace firmament {
namespace sim {

EventManager::EventManager(SimulatedWallTime* simulated_time) :
  simulated_time_(simulated_time), num_events_processed_(0) {
  LOG(INFO) << "Maximum number of task events to process: " << FLAGS_max_events;
  LOG(INFO) << "Maximum number of scheduling rounds: "
            << FLAGS_max_scheduling_rounds;
}

EventManager::~EventManager() {
}

void EventManager::AddEvent(uint64_t timestamp, EventDescriptor event) {
  events_.insert(pair<uint64_t, EventDescriptor>(timestamp, event));
}

pair<uint64_t, EventDescriptor> EventManager::GetNextEvent() {
  num_events_processed_++;
  multimap<uint64_t, EventDescriptor>::iterator it = events_.begin();
  pair<uint64_t, EventDescriptor> time_event = *it;
  events_.erase(it);
  simulated_time_->UpdateCurrentTimestampIfSmaller(time_event.first);
  return time_event;
}

uint64_t EventManager::GetTimeOfNextEvent() {
  multimap<uint64_t, EventDescriptor>::iterator it = events_.begin();
  if (it == events_.end()) {
    // Empty collection.
    return UINT64_MAX;
  } else {
    return it->first;
  }
}

uint64_t EventManager::GetTimeOfNextSchedulerRun(
    uint64_t cur_run_scheduler_at,
    uint64_t cur_scheduler_runtime) {
  if (FLAGS_batch_step == 0) {
    // We're in online mode.
    // Adjust for time warp factor.
    cur_scheduler_runtime *= FLAGS_online_factor;
    cur_run_scheduler_at += cur_scheduler_runtime;
    if (cur_scheduler_runtime == 0) {
      // The scheduler didn't have anything to do.
      // Only run it after the next event that can change task placement.
      for (auto& time_event : events_) {
        if (time_event.second.type() == EventDescriptor::TASK_SUBMIT ||
            time_event.second.type() == EventDescriptor::REMOVE_MACHINE ||
            time_event.second.type() == EventDescriptor::ADD_MACHINE ||
            time_event.second.type() == EventDescriptor::TASK_END_RUNTIME) {
          return time_event.first;
        }
      }
      // There's no event left that requires a scheduler run.
      return UINT64_MAX;
    }
  } else {
    // We're in batch mode.
    cur_run_scheduler_at += FLAGS_batch_step;
  }
  return cur_run_scheduler_at;
}

bool EventManager::HasSimulationCompleted(uint64_t num_scheduling_rounds) {
  // We only run for the first FLAGS_runtime microseconds.
  if (FLAGS_runtime / FLAGS_trace_speed_up < GetTimeOfNextEvent()) {
    LOG(INFO) << "Terminating at : " << simulated_time_->GetCurrentTimestamp();
    return true;
  }
  if (num_events_processed_ > FLAGS_max_events) {
    LOG(INFO) << "Terminating after " << num_events_processed_ << " events";
    return true;
  }
  if (num_scheduling_rounds >= FLAGS_max_scheduling_rounds) {
    LOG(INFO) << "Terminating after " << num_scheduling_rounds
              << " scheduling rounds.";
    return true;
  }
  return events_.begin() == events_.end();
}

void EventManager::RemoveTaskEndRuntimeEvent(
    const TraceTaskIdentifier& task_identifier,
    uint64_t task_end_time) {
  // Remove the task end time event from the simulator events_.
  pair<multimap<uint64_t, EventDescriptor>::iterator,
       multimap<uint64_t, EventDescriptor>::iterator> range_it =
    events_.equal_range(task_end_time);
  for (; range_it.first != range_it.second; range_it.first++) {
    if (range_it.first->second.type() == EventDescriptor::TASK_END_RUNTIME &&
        range_it.first->second.job_id() == task_identifier.job_id &&
        range_it.first->second.task_index() == task_identifier.task_index) {
      break;
    }
  }
  // We've found the event.
  if (range_it.first != range_it.second) {
    events_.erase(range_it.first);
  }
}

} // namespace sim
} // namespace firmament
