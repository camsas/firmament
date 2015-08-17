// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/trace-extract/google_trace_utils.h"

#include <algorithm>

namespace firmament {
namespace sim {

void LogStartOfSolverRun(FILE* graph_output,
                         shared_ptr<FlowGraph> flow_graph,
                         uint64_t run_solver_at) {
  LOG(INFO) << "Scheduler run for time: " << run_solver_at;
  LOG(INFO) << "Nodes: " << flow_graph->NumNodes()
            << ", arcs: " << flow_graph->NumArcs();
  if (graph_output) {
    fprintf(graph_output, "c SOI %jd\n", run_solver_at);
    fflush(graph_output);
  }
}

void LogSolverRunStats(uint64_t first_exogenous_event_seen,
                       FILE* stats_file,
                       const boost::timer::cpu_timer timer,
                       uint64_t solver_executed_at,
                       double algorithm_time,
                       double flow_solver_time,
                       const DIMACSChangeStats& change_stats) {
  if (stats_file) {
    boost::timer::cpu_times total_runtime_cpu_times = timer.elapsed();
    // TODO(ionel): Use misc/units.h
    boost::timer::nanosecond_type second = 1000*1000*1000;
    double total_runtime = total_runtime_cpu_times.wall;
    total_runtime /= second;
    if (FLAGS_batch_step == 0) {
      // online mode
      double scheduling_latency = solver_executed_at;
      scheduling_latency += algorithm_time * 1000 * 1000;
      scheduling_latency -= first_exogenous_event_seen;
      scheduling_latency /= (1000 * 1000);

      // will be negative if we have not seen any exogeneous event
      scheduling_latency = max(0.0, scheduling_latency);

      fprintf(stats_file, "%jd,%lf,%lf,%lf,%lf,", solver_executed_at,
              scheduling_latency, algorithm_time, flow_solver_time,
              total_runtime);
    } else {
      // batch mode
      fprintf(stats_file, "%jd,%lf%lf%lf,", solver_executed_at,
              algorithm_time, flow_solver_time, total_runtime);
    }
    OutputChangeStats(stats_file, change_stats);
    fflush(stats_file);
  }
}

void OutputChangeStats(FILE* stats_file, const DIMACSChangeStats& stats) {
  fprintf(stats_file, "%jd,%jd,%jd,%jd,%jd,%jd\n", stats.total_,
          stats.nodes_added_, stats.nodes_removed_, stats.arcs_added_,
          stats.arcs_changed_, stats.arcs_removed_);
}

void OutputStatsHeader(FILE* stats_file) {
  if (stats_file) {
    if (FLAGS_batch_step == 0) {
      // online
      fprintf(stats_file, "cluster_timestamp,scheduling_latency,"
              "algorithm_time,flow_solver_time,total_time,");
    } else {
      // batch
      fprintf(stats_file, "cluster_timestamp,algorithm_time,flow_solver_time,"
              "total_time,");
    }
    fprintf(stats_file, "total_changes,new_node,remove_node,new_arc,"
            "change_arc,remove_arc\n");
  }
}

void PrintResourceStats(uint64_t id, WhareMapStats* wms) {
  LOG(INFO) << "Node: " << id << " " << wms->num_devils() << " "
            << wms->num_rabbits() << " " << wms->num_sheep() << " "
            << wms->num_turtles();
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
