// Copyright (c) 2015 Ionel Gog  <ionel.gog@cl.cam.ac.uk>

#include "base/common.h"
#include "sim/google_trace_task_processor.h"

DEFINE_string(trace_path, "", "Path where the trace files are.");
DEFINE_bool(aggregate_task_usage, false, "Generate aggregated task usage.");
DEFINE_bool(jobs_runtime, false, "Generate task events with runtime.");
DEFINE_bool(jobs_num_tasks, false, "Generate num tasks for each jobs.");
DEFINE_int32(num_files_to_process, 500, "Number of files to process.");
DEFINE_bool(tasks_preemption_bins, false,
            "Compute bins of number of preempted tasks.");
DEFINE_string(task_bins_output, "bins.out",
              "The path to the file in which the task bins are written.");
DEFINE_int32(bin_by_event, 2,
             "Type of Google trace event to bin by."); // 2 == EVICT EVENT

inline void init(int argc, char *argv[]) {
  // Set up usage message.
  string usage("Sample usage:\nsun_simple_cost_scaling");
  google::SetUsageMessage(usage);

  // Use gflags to parse command line flags
  // The final (boolean) argument determines whether gflags-parsed flags should
  // be removed from the array (if true), otherwise they will re-ordered such
  // that all gflags-parsed flags are at the beginning.
  google::ParseCommandLineFlags(&argc, &argv, false);

  // Set up glog for logging output
  google::InitGoogleLogging(argv[0]);
}

int main(int argc, char *argv[]) {
  init(argc, argv);
  FLAGS_logtostderr = true;
  FLAGS_stderrthreshold = 0;
  firmament::sim::GoogleTraceTaskProcessor task_processor(FLAGS_trace_path);
  task_processor.Run();
  if (FLAGS_tasks_preemption_bins) {
    FILE* out_file = fopen(FLAGS_task_bins_output.c_str(), "w");
    if (out_file) {
      task_processor.BinTasksByEventType(FLAGS_bin_by_event, out_file);
      fclose(out_file);
    } else {
      LOG(FATAL) << "Could not open for writing bin output file "
                 << FLAGS_task_bins_output << ", error: " << strerror(errno);
    }
  }
  return 0;
}
