// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/trace-extract/google_trace_utils.h"

#include <algorithm>

#include "base/units.h"
#include "misc/utils.h"

DEFINE_double(events_fraction, 1.0, "Fraction of events to retain.");

namespace firmament {
namespace sim {

void LogStartOfSolverRun(FILE* graph_output,
                         uint64_t run_solver_at) {
  LOG(INFO) << "Scheduler run for time: " << run_solver_at;
  if (graph_output) {
    fprintf(graph_output, "c SOI %jd\n", run_solver_at);
    fflush(graph_output);
  }
}

void LogSolverRunStats(double avg_event_timestamp_in_scheduling_round,
                       FILE* stats_file,
                       const boost::timer::cpu_timer timer,
                       uint64_t solver_executed_at,
                       const scheduler::SchedulerStats& scheduler_stats) {
  if (stats_file) {
    boost::timer::cpu_times total_runtime_cpu_times = timer.elapsed();
    double total_runtime = total_runtime_cpu_times.wall;
    total_runtime /= SECONDS_TO_NANOSECONDS;
    if (FLAGS_batch_step == 0) {
      // online mode
      double scheduling_latency = solver_executed_at;
      scheduling_latency += scheduler_stats.algorithm_runtime *
        SECONDS_TO_MICROSECONDS;
      scheduling_latency -= avg_event_timestamp_in_scheduling_round;
      scheduling_latency /= SECONDS_TO_MICROSECONDS;

      // will be negative if we have not seen any event.
      scheduling_latency = max(0.0, scheduling_latency);

      fprintf(stats_file, "%jd,%lf,%lf,%lf,%lf,", solver_executed_at,
              scheduling_latency, scheduler_stats.algorithm_runtime,
              scheduler_stats.scheduler_runtime, total_runtime);
    } else {
      // batch mode
      fprintf(stats_file, "%jd,%lf%lf%lf,", solver_executed_at,
              scheduler_stats.algorithm_runtime,
              scheduler_stats.scheduler_runtime, total_runtime);
    }
    fflush(stats_file);
  }
}

uint64_t MaxEventIdToRetain() {
  // We must check if we're retaining all events. If so, we have to return
  // UINT64_MAX because otherwise we might end up overflowing.
  if (IsEqual(FLAGS_events_fraction, 1.0)) {
    return UINT64_MAX;
  } else {
    return FLAGS_events_fraction * UINT64_MAX;
  }
}

void OutputStatsHeader(FILE* stats_file) {
  if (stats_file) {
    if (FLAGS_batch_step == 0) {
      // online
      fprintf(stats_file, "cluster_timestamp,scheduling_latency,"
              "algorithm_runtime,scheduler_runtime,total_time\n");
    } else {
      // batch
      fprintf(stats_file, "cluster_timestamp,algorithm_runtime,"
              "scheduler_runtime,total_time\n");
    }
  }
}

EventDescriptor_EventType TranslateMachineEvent(
    int32_t machine_event) {
  if (machine_event == MACHINE_ADD) {
    return EventDescriptor::ADD_MACHINE;
  } else if (machine_event == MACHINE_REMOVE) {
    return EventDescriptor::REMOVE_MACHINE;
  } else if (machine_event == MACHINE_UPDATE) {
    return EventDescriptor::UPDATE_MACHINE;
  } else {
    LOG(FATAL) << "Unexpected machine event type: " << machine_event;
  }
}

}  // namespace sim
}  // namespace firmament
