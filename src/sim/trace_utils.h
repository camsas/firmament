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

#ifndef FIRMAMENT_SIM_TRACE_UTILS_H
#define FIRMAMENT_SIM_TRACE_UTILS_H

#include <boost/timer/timer.hpp>

#include <string>

#include "base/common.h"
#include "misc/trace_generator.h"
#include "scheduling/scheduler_interface.h"
#include "sim/event_desc.pb.h"

DECLARE_uint64(batch_step);
DECLARE_bool(graph_output_events);

namespace firmament {
namespace sim {

static const uint64_t kSeed = 0;

struct TraceTaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TraceTaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TraceTaskIdentifierHasher {
  size_t operator()(const TraceTaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id) * 17 + hash<uint64_t>()(key.task_index);
  }
};

struct TraceTaskStats {
  TraceTaskStats() : avg_mean_cpu_usage_(0), avg_canonical_mem_usage_(0),
    avg_assigned_mem_usage_(0), avg_unmapped_page_cache_(0),
    avg_total_page_cache_(0), avg_mean_disk_io_time_(0),
    avg_mean_local_disk_used_(0), avg_cpi_(0), avg_mai_(0),
    total_runtime_(0) {
  }
  double avg_mean_cpu_usage_;
  double avg_canonical_mem_usage_;
  double avg_assigned_mem_usage_;
  double avg_unmapped_page_cache_;
  double avg_total_page_cache_;
  double avg_mean_disk_io_time_;
  double avg_mean_local_disk_used_;
  double avg_cpi_;
  double avg_mai_;
  uint64_t total_runtime_;
};

TaskID_t GenerateTaskIDFromTraceIdentifier(const TraceTaskIdentifier& ti);
void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl);
uint64_t MaxEventIdToRetain();

EventDescriptor_EventType TranslateMachineEvent(int32_t machine_event);

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_UTILS_H
