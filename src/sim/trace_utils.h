// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
  double avg_mean_cpu_usage_;
  double avg_canonical_mem_usage_;
  double avg_assigned_mem_usage_;
  double avg_unmapped_page_cache_;
  double avg_total_page_cache_;
  double avg_mean_disk_io_time_;
  double avg_mean_local_disk_used_;
  double avg_cpi_;
  double avg_mai_;
};

TaskID_t GenerateTaskIDFromTraceIdentifier(const TraceTaskIdentifier& ti);
void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl);
uint64_t MaxEventIdToRetain();

EventDescriptor_EventType TranslateMachineEvent(int32_t machine_event);

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_UTILS_H
