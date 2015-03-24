// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Generate Google style trace.

#ifndef FIRMAMENT_MISC_GENERATE_TRACE_H
#define FIRMAMENT_MISC_GENERATE_TRACE_H

#include "base/types.h"

namespace firmament {

typedef struct TaskRuntime_st {
  uint64_t start_time;
  uint64_t num_runs;
  uint64_t last_schedule_time;
  uint64_t total_runtime;
  uint64_t runtime;
  int64_t scheduling_class;
  int64_t priority;
  double cpu_request;
  double ram_request;
  double disk_request;
  int32_t machine_constraint;
} TaskRuntime;

class GenerateTrace {
 public:
  GenerateTrace();
  ~GenerateTrace();
  void AddMachine(ResourceID_t res_id);
  void RemoveMachine(ResourceID_t res_id);
  void TaskSubmitted(JobID_t job_id, TaskID_t task_id);
  void TaskCompleted(TaskID_t task_id);
  void TaskEvicted(TaskID_t task_id);
  void TaskFailed(TaskID_t task_id);
  void TaskKilled(TaskID_t task_id);
 private:
  unordered_map<TaskID_t, uint64_t> task_to_job_;
  unordered_map<uint64_t, uint64_t> job_num_tasks_;
  unordered_map<TaskID_t, uint64_t> task_to_index_;
  unordered_map<TaskID_t, TaskRuntime> task_to_runtime_;
  FILE* machine_events_;
  FILE* task_events_;
  FILE* task_runtime_events_;
  FILE* jobs_num_tasks_;
  FILE* task_usage_stat_;
};

} // namespace firmament

#endif  // FIRMAMENT_MISC_GENERATE_TRACE_H
