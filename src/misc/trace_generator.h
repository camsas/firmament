// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Generate Google style trace.

#ifndef FIRMAMENT_MISC_TRACE_GENERATOR_H
#define FIRMAMENT_MISC_TRACE_GENERATOR_H

#include "base/types.h"
#include "misc/time_interface.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/scheduler_interface.h"

namespace firmament {

// Google trace events. The definition and value of each event are documented
// at https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md.
enum TraceTaskEvent {
  TASK_SUBMIT_EVENT = 0,
  TASK_SCHEDULE_EVENT = 1,
  TASK_EVICT_EVENT = 2,
  TASK_FAIL_EVENT = 3,
  TASK_FINISH_EVENT = 4,
  TASK_KILL_EVENT = 5,
  TASK_LOST_EVENT = 6,
  TASK_UPDATE_PENDING_EVENT = 7,
  TASK_UPDATE_RUNNING_EVENT = 8,
};

enum TraceMachineEvent {
  MACHINE_ADD = 0,
  MACHINE_REMOVE = 1,
  MACHINE_UPDATE = 2
};

struct TaskRuntime {
  TaskRuntime() : task_id_(0), start_time_(0), num_runs_(0),
    last_schedule_time_(0), total_runtime_(0), runtime_(0),
    scheduling_class_(0), priority_(0), cpu_request_(0), ram_request_(0),
    disk_request_(0), machine_constraint_(0) {
  }

  // The id of the task. When we run a simulation, the field  gets populated
  // with the trace task id instead of Firmanent task id.
  uint64_t task_id_;
  uint64_t start_time_;
  uint64_t num_runs_;
  uint64_t last_schedule_time_;
  uint64_t total_runtime_;
  uint64_t runtime_;
  // TODO(ionel): Populate the remainder of the fields.
  int64_t scheduling_class_;
  int64_t priority_;
  double cpu_request_;
  double ram_request_;
  double disk_request_;
  int32_t machine_constraint_;
};

class TraceGenerator {
 public:
  explicit TraceGenerator(TimeInterface* time_manager);
  ~TraceGenerator();
  void AddMachine(const ResourceDescriptor& rd);
  void RemoveMachine(const ResourceDescriptor& rd);
  void SchedulerRun(const scheduler::SchedulerStats& scheduler_stats,
                    const DIMACSChangeStats& dimacs_stats);
  void TaskSubmitted(TaskDescriptor* td_ptr);
  void TaskCompleted(TaskID_t task_id);
  void TaskEvicted(TaskID_t task_id);
  void TaskFailed(TaskID_t task_id);
  void TaskKilled(TaskID_t task_id);
  void TaskScheduled(TaskID_t task_id, ResourceID_t res_id);

 private:
  uint64_t GetMachineId(const ResourceDescriptor& rd);

  TimeInterface* time_manager_;
  unordered_map<TaskID_t, uint64_t> task_to_job_;
  // We currently only remove data from job_num_tasks_ when
  // TraceGenerator is deleted. Update the code if the table
  // ends up consuming too much memory.
  unordered_map<uint64_t, uint64_t> job_num_tasks_;
  unordered_map<TaskID_t, TaskRuntime> task_to_runtime_;
  FILE* machine_events_;
  FILE* scheduler_events_;
  FILE* task_events_;
  FILE* task_runtime_events_;
  FILE* jobs_num_tasks_;
  FILE* task_usage_stat_;
};

} // namespace firmament

#endif  // FIRMAMENT_MISC_TRACE_GENERATOR_H
