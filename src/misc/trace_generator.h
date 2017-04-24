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
  TASK_REMOVED_EVENT = 9,
};

enum TraceMachineEvent {
  MACHINE_ADD = 0,
  MACHINE_REMOVE = 1,
  MACHINE_UPDATE = 2
};

enum TraceDFSEvent {
  BLOCK_ADD = 0,
  BLOCK_REMOVE = 1
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
  void AddBlock(ResourceID_t machine_res_id, uint64_t block_id,
                uint64_t block_size);
  void AddMachine(const ResourceDescriptor& rd);
  void AddMachineToRack(ResourceID_t machine_res_id, uint64_t rack_id);
  void AddTaskInputBlock(const TaskDescriptor& td, uint64_t block_id);
  void AddTaskQuincy(const TaskDescriptor& td, uint64_t input_size,
                     int64_t worst_cluster_cost, int64_t best_rack_cost,
                     int64_t best_machine_cost, int64_t cost_to_unsched,
                     uint64_t num_pref_machines, uint64_t num_pref_racks);
  void RemoveBlock(ResourceID_t machine_res_id, uint64_t block_id,
                   uint64_t block_size);
  void RemoveMachine(const ResourceDescriptor& rd);
  void RemoveMachineFromRack(ResourceID_t machine_res_id, uint64_t rack_id);
  void SchedulerRun(const scheduler::SchedulerStats& scheduler_stats,
                    const DIMACSChangeStats& dimacs_stats);
  void TaskSubmitted(TaskDescriptor* td_ptr);
  void TaskCompleted(TaskID_t task_id, const ResourceDescriptor& rd);
  void TaskEvicted(TaskID_t task_id, const ResourceDescriptor& rd,
                   bool migrated);
  void TaskFailed(TaskID_t task_id, const ResourceDescriptor& rd);
  void TaskKilled(TaskID_t task_id, const ResourceDescriptor& rd);
  void TaskRemoved(TaskID_t task_id, bool was_running);
  void TaskMigrated(TaskDescriptor* td_ptr,
                    const ResourceDescriptor& old_rd,
                    const ResourceDescriptor& new_rd);
  void TaskScheduled(TaskID_t task_id, const ResourceDescriptor& rd);

 private:
  uint64_t GetMachineId(const ResourceDescriptor& rd);

  TimeInterface* time_manager_;
  unordered_map<TaskID_t, uint64_t> task_to_job_;
  // We currently only remove data from job_num_tasks_ when
  // TraceGenerator is deleted. Update the code if the table
  // ends up consuming too much memory.
  unordered_map<uint64_t, uint64_t> job_num_tasks_;
  unordered_map<TaskID_t, TaskRuntime> task_to_runtime_;
  unordered_map<ResourceID_t, uint64_t,
      boost::hash<ResourceID_t>> machine_res_id_to_trace_id_;
  FILE* machine_events_;
  FILE* scheduler_events_;
  FILE* task_events_;
  FILE* task_runtime_events_;
  FILE* jobs_num_tasks_;
  FILE* task_usage_stat_;
  FILE* dfs_events_;
  FILE* tasks_to_blocks_;
  FILE* machines_to_racks_;
  FILE* quincy_tasks_;
  uint64_t unscheduled_tasks_cnt_;
  uint64_t running_tasks_cnt_;
  uint64_t evicted_tasks_cnt_;
  uint64_t migrated_tasks_cnt_;
  // It includes task submissions, completions, evictions, failures, kills,
  // placements and migrations
  uint64_t task_events_cnt_per_round_;
  // It includes machine additions and removals.
  uint64_t machine_events_cnt_per_round_;
};

} // namespace firmament

#endif  // FIRMAMENT_MISC_TRACE_GENERATOR_H
