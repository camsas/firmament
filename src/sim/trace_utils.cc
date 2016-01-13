// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "sim/trace_utils.h"

#include <fcntl.h>

#include <boost/filesystem.hpp>

#include <algorithm>

#include "base/units.h"
#include "misc/utils.h"

DEFINE_string(machine_tmpl_file, "../../tests/testdata/machine_topo.pbin",
              "File specifying machine topology. (Note: the given path must be "
              "relative to the directory of the binary)");

namespace firmament {
namespace sim {

void LoadMachineTemplate(ResourceTopologyNodeDescriptor* machine_tmpl) {
  boost::filesystem::path machine_tmpl_path(FLAGS_machine_tmpl_file);
  if (machine_tmpl_path.is_relative()) {
    // lookup file relative to directory of binary, not CWD
    char binary_path[1024];
    size_t bytes = ExecutableDirectory(binary_path, sizeof(binary_path));
    CHECK(bytes < sizeof(binary_path));
    boost::filesystem::path binary_path_boost(binary_path);
    binary_path_boost.remove_filename();

    machine_tmpl_path = binary_path_boost / machine_tmpl_path;
  }

  string machine_tmpl_fname(machine_tmpl_path.string());
  LOG(INFO) << "Loading machine descriptor from " << machine_tmpl_fname;
  int fd = open(machine_tmpl_fname.c_str(), O_RDONLY);
  if (fd < 0) {
    PLOG(FATAL) << "Could not load " << machine_tmpl_fname;
  }
  machine_tmpl->ParseFromFileDescriptor(fd);
  close(fd);
}

void LogStartOfSchedulerRun(FILE* graph_output,
                            uint64_t run_scheduler_at) {
  LOG(INFO) << "Scheduler run for time: " << run_scheduler_at;
  if (graph_output) {
    fprintf(graph_output, "c SOI %jd\n", run_scheduler_at);
    fflush(graph_output);
  }
}

void LogSchedulerRunStats(double avg_event_timestamp_in_scheduling_round,
                          FILE* stats_file,
                          const boost::timer::cpu_timer timer,
                          uint64_t scheduler_executed_at,
                          const scheduler::SchedulerStats& scheduler_stats) {
  if (stats_file) {
    uint64_t total_runtime = timer.elapsed().wall / NANOSECONDS_IN_MICROSECOND;
    if (FLAGS_batch_step == 0) {
      // online mode
      uint64_t scheduling_latency = 0;
      if (avg_event_timestamp_in_scheduling_round >= 0 &&
          scheduler_stats.algorithm_runtime < numeric_limits<uint64_t>::max()) {
        // Set scheduling latency only if we've seen an event and
        // if we have the runtime of the algorithm.
        scheduling_latency = scheduler_executed_at +
          scheduler_stats.algorithm_runtime -
          avg_event_timestamp_in_scheduling_round;
      }

      fprintf(stats_file, "%ju,%ju,%ju,%ju,%ju,", scheduler_executed_at,
              scheduling_latency, scheduler_stats.algorithm_runtime,
              scheduler_stats.scheduler_runtime, total_runtime);
    } else {
      // batch mode
      fprintf(stats_file, "%ju,%ju%ju%ju,", scheduler_executed_at,
              scheduler_stats.algorithm_runtime,
              scheduler_stats.scheduler_runtime, total_runtime);
    }
    fflush(stats_file);
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
