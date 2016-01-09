// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H
#define FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

using namespace std; // NOLINT

namespace firmament {
namespace sim {

struct TaskSchedulingEvent {
  uint64_t job_id;
  uint64_t task_index;
  int32_t event_type;
};

struct TaskResourceUsage {
  TaskResourceUsage() : mean_cpu_usage(0), canonical_mem_usage(0),
    assigned_mem_usage(0), unmapped_page_cache(0), total_page_cache(0),
    max_mem_usage(0), mean_disk_io_time(0), mean_local_disk_used(0),
    max_cpu_usage(0), max_disk_io_time(0), cpi(0), mai(0) {
  }

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
};

struct TaskResourceUsageStats {
  TaskResourceUsageStats() : num_mean_cpu_usage(0), num_canonical_mem_usage(0),
    num_assigned_mem_usage(0), num_unmapped_page_cache(0),
    num_total_page_cache(0), num_max_mem_usage(0), num_mean_disk_io_time(0),
    num_mean_local_disk_used(0), num_max_cpu_usage(0), num_max_disk_io_time(0),
    num_cpi(0), num_mai(0) {
  }

  TaskResourceUsage avg_usage;
  TaskResourceUsage min_usage;
  TaskResourceUsage max_usage;
  TaskResourceUsage variance_usage;
  // The number of samples used to calculate the statistics.
  uint32_t num_mean_cpu_usage;
  uint32_t num_canonical_mem_usage;
  uint32_t num_assigned_mem_usage;
  uint32_t num_unmapped_page_cache;
  uint32_t num_total_page_cache;
  uint32_t num_max_mem_usage;
  uint32_t num_mean_disk_io_time;
  uint32_t num_mean_local_disk_used;
  uint32_t num_max_cpu_usage;
  uint32_t num_max_disk_io_time;
  uint32_t num_cpi;
  uint32_t num_mai;
};

struct TaskRuntime {
  int64_t start_time;
  uint64_t num_runs;
  int64_t last_schedule_time;
  int64_t total_runtime;
  int64_t scheduling_class;
  int64_t priority;
  double cpu_request;
  double ram_request;
  double disk_request;
  int32_t machine_constraint;
};

struct TaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id) * 17 +
      hash<uint64_t>()(key.task_index);
  }
};

class GoogleTraceTaskProcessor {
 public:
  explicit GoogleTraceTaskProcessor(const string& trace_path);

  void AggregateTaskUsage();

  /**
   * Compute the number of events of a particular type withing each time
   * interval.
   */
  void BinTasksByEventType(int32_t event_type, FILE* out_file); // NOLINT

  /**
   * Generate task events with runtime information.
   * NOTE: Events will only be generated for tasks that successfully complete.
   */
  void JobsRuntimeEvents();

  /**
   * Compute the number of tasks each job has.
   */
  void JobsNumTasks();

  void Run();

 private:
  TaskResourceUsage BuildTaskResourceUsage(vector<string>& line_cols); // NOLINT
  void ExpandTaskEvent(
      uint64_t timestamp, const TaskIdentifier& task_id, int32_t event_type,
      unordered_map<TaskIdentifier, TaskRuntime,
                    TaskIdentifierHasher>* tasks_runtime,
      unordered_map<uint64_t, string>* job_id_to_name, FILE* out_events_file,
      vector<string>& line_cols); // NOLINT
  void InitializeResourceUsageStats(TaskResourceUsageStats* usage_stats);
  void PopulateTaskRuntime(TaskRuntime* task_runtime_ptr,
                           vector<string>& cols); // NOLINT
  void PrintStats(FILE* usage_stat_file, const TaskIdentifier& task_id,
                  TaskResourceUsageStats* task_resource);
  void PrintTaskRuntime(FILE* out_events_file, const TaskRuntime& task_runtime,
                        const TaskIdentifier& task_id,
                        string logical_job_name, uint64_t runtime);
  void ProcessSchedulingEvents(
      uint64_t timestamp,
      multimap<uint64_t, TaskSchedulingEvent>* scheduling_events,
      unordered_map<TaskIdentifier, TaskResourceUsageStats,
                    TaskIdentifierHasher>* task_usage,
      unordered_set<TaskIdentifier, TaskIdentifierHasher>* finished_tasks,
      FILE* usage_stat_file);
  unordered_map<uint64_t, string>& ReadLogicalJobsName();
  multimap<uint64_t, TaskSchedulingEvent>& ReadTaskStateChangingEvents(
      unordered_map<uint64_t, uint64_t>* job_num_tasks);
  void UpdateStats(double task_usage, double* min_usage, double* max_usage,
                   double* avg_usage, double* variance_usage,
                   uint32_t* num_usage);
  void UpdateUsageStats(const TaskResourceUsage& task_resource_usage,
                        TaskResourceUsageStats* usage_stats);

  string trace_path_;
};

} // namespace sim
} // namespace firmament
#endif // FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H
