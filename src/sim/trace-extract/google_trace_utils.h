// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_UTILS_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_UTILS_H

#include <boost/timer/timer.hpp>

#include <string>

#include "base/common.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"
#include "sim/trace-extract/event_desc.pb.h"

DECLARE_uint64(batch_step);
DECLARE_bool(graph_output_events);

namespace firmament {
namespace sim {

// Google trace events. The definition and value of each event are documented
// at https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md.
#define SUBMIT_EVENT 0
#define SCHEDULE_EVENT 1
#define EVICT_EVENT 2
#define FAIL_EVENT 3
#define FINISH_EVENT 4
#define KILL_EVENT 5
#define LOST_EVENT 6
#define UPDATE_PENDING_EVENT 7
#define UPDATE_RUNNING_EVENT 8

#define MACHINE_ADD 0
#define MACHINE_REMOVE 1
#define MACHINE_UPDATE 2

static const uint64_t kSeed = 0;

struct TaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id) * 17 + hash<uint64_t>()(key.task_index);
  }
};

struct TaskStats {
  double avg_mean_cpu_usage;
  double avg_canonical_mem_usage;
  double avg_assigned_mem_usage;
  double avg_unmapped_page_cache;
  double avg_total_page_cache;
  double avg_mean_disk_io_time;
  double avg_mean_local_disk_used;
  double avg_cpi;
  double avg_mai;
};

inline void LogEvent(FILE* graph_output, const string& msg) {
  VLOG(1) << msg;
  if (graph_output && FLAGS_graph_output_events) {
    fprintf(graph_output, "c %s\n", msg.c_str());
  }
}

void LogSolverRunStats(double avg_event_timestamp_in_scheduling_round,
                       FILE* stats_file,
                       const boost::timer::cpu_timer timer,
                       uint64_t solver_executed_at,
                       double algorithm_time,
                       double flow_solver_time,
                       const DIMACSChangeStats& change_stats);

void LogStartOfSolverRun(FILE* graph_output,
                         shared_ptr<FlowGraph> flow_graph,
                         uint64_t run_solver_at);

uint64_t MaxEventIdToRetain();

void OutputChangeStats(FILE* stats_file, const DIMACSChangeStats& stats);

void OutputStatsHeader(FILE* stats_file);

void PrintResourceStats(uint64_t id, WhareMapStats* wms);

EventDescriptor_EventType TranslateMachineEvent(int32_t machine_event);

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_UTILS_H
