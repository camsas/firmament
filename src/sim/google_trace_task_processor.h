// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H
#define FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H

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
  int64_t scheduling_class;
  int64_t priority;
  double cpu_request;
  double ram_request;
  double disk_request;
  int32_t machine_constraint;
} TaskRuntime;

struct TaskIdentifier {
  uint64_t job_id;
  uint64_t task_index;

  bool operator==(const TaskIdentifier& other) const {
    return job_id == other.job_id && task_index == other.task_index;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return std::hash<uint64_t>()(key.job_id) * 17 +
      std::hash<uint64_t>()(key.task_index);
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
  TaskResourceUsage AvgTaskUsage(
      const vector<TaskResourceUsage>& resource_usage);
  TaskResourceUsage BuildTaskResourceUsage(vector<string>& line_cols); // NOLINT
  void ExpandTaskEvent(
      uint64_t timestamp, const TaskIdentifier& task_id, int32_t event_type,
      unordered_map<TaskIdentifier, TaskRuntime,
                    TaskIdentifierHasher>* tasks_runtime,
      unordered_map<uint64_t, string>* job_id_to_name, FILE* out_events_file,
      vector<string>& line_cols); // NOLINT
  TaskResourceUsage MaxTaskUsage(
      const vector<TaskResourceUsage>& resource_usage);
  TaskResourceUsage MinTaskUsage(
      const vector<TaskResourceUsage>& resource_usage);
  void PopulateTaskRuntime(TaskRuntime* task_runtime_ptr,
                           vector<string>& cols); // NOLINT
  void PrintStats(FILE* usage_stat_file, const TaskIdentifier& task_id,
                  const vector<TaskResourceUsage>& task_resource);
  void PrintTaskRuntime(FILE* out_events_file, const TaskRuntime& task_runtime,
                        const TaskIdentifier& task_id,
                        string logical_job_name, uint64_t runtime);
  void ProcessSchedulingEvents(
      uint64_t timestamp,
      multimap<uint64_t, TaskSchedulingEvent>* scheduling_events,
      unordered_map<TaskIdentifier, vector<TaskResourceUsage>,
                    TaskIdentifierHasher>* task_usage,
      FILE* usage_stat_file);
  unordered_map<uint64_t, string>& ReadLogicalJobsName();
  multimap<uint64_t, TaskSchedulingEvent>& ReadTaskStateChangingEvents(
      unordered_map<uint64_t, uint64_t>* job_num_tasks);
  TaskResourceUsage StandardDevTaskUsage(
      const vector<TaskResourceUsage>& resource_usage);

  string trace_path_;
};

} // namespace sim
} // namespace firmament
#endif // FIRMAMENT_SIM_GOOGLE_TRACE_TASK_PROCESSOR_H
