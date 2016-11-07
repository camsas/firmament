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
  uint64_t job_id_;
  uint64_t task_index_;
  int32_t event_type_;
};

struct TaskResourceUsage {
  TaskResourceUsage() : mean_cpu_usage_(0), canonical_mem_usage_(0),
    assigned_mem_usage_(0), unmapped_page_cache_(0), total_page_cache_(0),
    max_mem_usage_(0), mean_disk_io_time_(0), mean_local_disk_used_(0),
    max_cpu_usage_(0), max_disk_io_time_(0), cpi_(0), mai_(0) {
  }

  double mean_cpu_usage_;
  double canonical_mem_usage_;
  double assigned_mem_usage_;
  double unmapped_page_cache_;
  double total_page_cache_;
  double max_mem_usage_;
  double mean_disk_io_time_;
  double mean_local_disk_used_;
  double max_cpu_usage_;
  double max_disk_io_time_;
  double cpi_;
  double mai_;
};

struct TaskResourceUsageStats {
  TaskResourceUsageStats() : sample_count_mean_cpu_usage_(0),
    sample_count_canonical_mem_usage_(0), sample_count_assigned_mem_usage_(0),
    sample_count_unmapped_page_cache_(0), sample_count_total_page_cache_(0),
    sample_count_max_mem_usage_(0), sample_count_mean_disk_io_time_(0),
    sample_count_mean_local_disk_used_(0), sample_count_max_cpu_usage_(0),
    sample_count_max_disk_io_time_(0), sample_count_cpi_(0),
    sample_count_mai_(0) {
  }

  TaskResourceUsage avg_usage_;
  TaskResourceUsage min_usage_;
  TaskResourceUsage max_usage_;
  TaskResourceUsage variance_usage_;
  // The number of samples used to calculate the statistics.
  uint32_t sample_count_mean_cpu_usage_;
  uint32_t sample_count_canonical_mem_usage_;
  uint32_t sample_count_assigned_mem_usage_;
  uint32_t sample_count_unmapped_page_cache_;
  uint32_t sample_count_total_page_cache_;
  uint32_t sample_count_max_mem_usage_;
  uint32_t sample_count_mean_disk_io_time_;
  uint32_t sample_count_mean_local_disk_used_;
  uint32_t sample_count_max_cpu_usage_;
  uint32_t sample_count_max_disk_io_time_;
  uint32_t sample_count_cpi_;
  uint32_t sample_count_mai_;
};

struct TaskRuntime {
  TaskRuntime() : start_time_(0), num_runs_(0), last_schedule_time_(-1),
    total_runtime_(0), scheduling_class_(0), priority_(0), cpu_request_(0),
    ram_request_(0), disk_request_(0), machine_constraint_(0), runtime_(0) {
  }
  int64_t start_time_;
  uint64_t num_runs_;
  int64_t last_schedule_time_;
  int64_t total_runtime_;
  int64_t scheduling_class_;
  int64_t priority_;
  double cpu_request_;
  double ram_request_;
  double disk_request_;
  int32_t machine_constraint_;
  uint64_t runtime_;
};

struct TaskIdentifier {
  uint64_t job_id_;
  uint64_t task_index_;

  bool operator==(const TaskIdentifier& other) const {
    return job_id_ == other.job_id_ && task_index_ == other.task_index_;
  }
};

struct TaskIdentifierHasher {
  size_t operator()(const TaskIdentifier& key) const {
    return hash<uint64_t>()(key.job_id_) * 17 +
      hash<uint64_t>()(key.task_index_);
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
      unordered_map<uint64_t, string>* job_id_to_name,
      vector<string>& line_cols); // NOLINT
  void InitializeResourceUsageStats(TaskResourceUsageStats* usage_stats);
  void PopulateTaskRuntime(TaskRuntime* task_runtime_ptr,
                           vector<string>& cols); // NOLINT
  void PrintStats(FILE* usage_stat_file, const TaskIdentifier& task_id,
                  const TaskResourceUsageStats& task_resource);
  void PrintTaskRuntime(FILE* out_events_file, const TaskRuntime& task_runtime,
                        const TaskIdentifier& task_id, string logical_job_name);
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
