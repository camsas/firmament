// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_TASK_PROCESSOR_H
#define FIRMAMENT_SIM_TRACE_EXTRACT_GOOGLE_TRACE_TASK_PROCESSOR_H

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std; // NOLINT

namespace firmament {
namespace sim {

typedef struct TaskSchedulingEvent_st {
  uint64_t job_id;
  uint64_t task_index;
  int32_t event_type;
} TaskSchedulingEvent;

typedef struct TaskResourceUsage_st {
  double mean_cpu_usage;
  double canonical_mem_usage;
  double assigned_mem_usage;
  double unmapped_page_cache;
  double total_page_cache;
  double max_mem_usage;
  double mean_disk_io_time;
  double mean_local_disk_used;
  double max_cpu_usage;
  double max_disk_io_time;
  double cpi;
  double mai;
} TaskResourceUsage;

typedef struct TaskRuntime_st {
  int64_t start_time;
  uint64_t num_runs;
  int64_t last_schedule_time;
  int64_t total_runtime;
} TaskRuntime;

class GoogleTraceTaskProcessor {
 public:
  explicit GoogleTraceTaskProcessor(const string& trace_path);

  void AggregateTaskUsage();
  void ExpandTaskEvents();
  void JobsNumTasks();
  void Run();

 private:
  TaskResourceUsage* AvgTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage);
  TaskResourceUsage* BuildTaskResourceUsage(vector<string>& line_cols); // NOLINT
  TaskResourceUsage* MaxTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage);
  TaskResourceUsage* MinTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage);
  void PrintStats(FILE* usage_stat_file, uint64_t job_id, uint64_t task_index,
                  const vector<TaskResourceUsage*>& task_resource);
  void PrintTaskRuntime(FILE* out_events_file, TaskRuntime* task_runtime,
                        uint64_t job_id, uint64_t task_index,
                        string logical_job_name, uint64_t runtime,
                        vector<string>& cols); // NOLINT
  map<uint64_t, string>& ReadLogicalJobsName();
  multimap<uint64_t, TaskSchedulingEvent>& ReadTaskSchedulingEvents(
      unordered_map<uint64_t, uint64_t>* job_num_tasks);
  TaskResourceUsage* StandardDevTaskUsage(
      const vector<TaskResourceUsage*>& resource_usage);

  string trace_path_;
};

} // namespace sim
} // namespace firmament
#endif
