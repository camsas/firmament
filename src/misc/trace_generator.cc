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

#include "misc/trace_generator.h"

#include <boost/functional/hash.hpp>
#include <SpookyV2.h>
#include <string>

#include "base/common.h"
#include "misc/map-util.h"
#include "misc/utils.h"

#define UNSCHEDULED_TASKS_WARNING_THRESHOLD 1.0 // Percentage

DEFINE_bool(generate_trace, false, "Generate Google style trace");
DEFINE_string(generated_trace_path, "",
              "Path to where the trace will be generated");
DEFINE_bool(generate_quincy_cost_model_trace, false,
            "A trace containing information specific to the Quincy cost model");

namespace firmament {

TraceGenerator::TraceGenerator(TimeInterface* time_manager)
  : time_manager_(time_manager), unscheduled_tasks_cnt_(0),
    running_tasks_cnt_(0), evicted_tasks_cnt_(0), migrated_tasks_cnt_(0),
    task_events_cnt_per_round_(0), machine_events_cnt_per_round_(0) {
  if (FLAGS_generate_trace) {
    MkdirIfNotPresent(FLAGS_generated_trace_path);
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/machine_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/scheduler_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_runtime_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/jobs_num_tasks");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_usage_stat");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/dfs_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/tasks_to_blocks");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/machines_to_racks");
    string path =
      FLAGS_generated_trace_path + "/machine_events/part-00000-of-00001.csv";
    machine_events_ = fopen(path.c_str(), "w");
    CHECK(machine_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path +
      "/scheduler_events/scheduler_events.csv";
    scheduler_events_ = fopen(path.c_str(), "w");
    CHECK(scheduler_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/task_events/part-00000-of-00500.csv";
    task_events_ = fopen(path.c_str(), "w");
    CHECK(task_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path +
      "/task_runtime_events/task_runtime_events.csv";
    task_runtime_events_ = fopen(path.c_str(), "w");
    CHECK(task_runtime_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/jobs_num_tasks/jobs_num_tasks.csv";
    jobs_num_tasks_ = fopen(path.c_str(), "w");
    CHECK(jobs_num_tasks_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/task_usage_stat/task_usage_stat.csv";
    task_usage_stat_ = fopen(path.c_str(), "w");
    CHECK(task_usage_stat_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/dfs_events/dfs_events.csv";
    dfs_events_ = fopen(path.c_str(), "w");
    CHECK(dfs_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/tasks_to_blocks/tasks_to_blocks.csv";
    tasks_to_blocks_ = fopen(path.c_str(), "w");
    CHECK(tasks_to_blocks_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "/machines_to_racks/machines_to_racks.csv";
    machines_to_racks_ = fopen(path.c_str(), "w");
    CHECK(machines_to_racks_ != NULL) << "Failed to open: " << path;
    if (FLAGS_generate_quincy_cost_model_trace) {
      MkdirIfNotPresent(FLAGS_generated_trace_path + "/quincy_tasks");
      path = FLAGS_generated_trace_path + "/quincy_tasks/quincy_tasks.csv";
      quincy_tasks_ = fopen(path.c_str(), "w");
      CHECK(quincy_tasks_ != NULL) << "Failed to open: " << path;
    }
  }
}

TraceGenerator::~TraceGenerator() {
  if (FLAGS_generate_trace) {
    fclose(machine_events_);
    fclose(scheduler_events_);
    fclose(task_events_);
    // Print runtime for service tasks or tasks that haven't completed.
    for (auto& task_id_runtime : task_to_runtime_) {
      uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id_runtime.first);
      TaskRuntime task_runtime = task_id_runtime.second;
      // NOTE: We are using the job id as the job logical name.
      fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
              *job_id_ptr, task_runtime.task_id_, *job_id_ptr,
              task_runtime.start_time_, task_runtime.total_runtime_,
              task_runtime.runtime_, task_runtime.num_runs_);
    }
    fclose(task_runtime_events_);
    // Print number of tasks for service jobs or jobs that haven't completed.
    for (auto& job_to_num_tasks : job_num_tasks_) {
      fprintf(jobs_num_tasks_, "%ju,%ju\n", job_to_num_tasks.first,
              job_to_num_tasks.second);
    }
    fclose(jobs_num_tasks_);
    // TODO(ionel): Collect task usage stats.
    // for (auto& task_to_job : task_to_job_) {
    //   uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_to_job.first);
    //   fprintf(task_usage_stat_, "%ju,%ju,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,"
    //           "0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0\n", *job_id_ptr,
    //           task_to_job.first);
    // }
    fclose(task_usage_stat_);
    fclose(dfs_events_);
    fclose(tasks_to_blocks_);
    fclose(machines_to_racks_);
    if (FLAGS_generate_quincy_cost_model_trace) {
      fclose(quincy_tasks_);
    }
  }
  // time_manager is not owned by this class. We don't have to delete it here.
}

void TraceGenerator::AddBlock(ResourceID_t machine_res_id,
                              uint64_t block_id, uint64_t block_size) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* machine_id =
      FindOrNull(machine_res_id_to_trace_id_, machine_res_id);
    CHECK_NOTNULL(machine_id);
    fprintf(dfs_events_, "%ju,%d,%ju,%ju,%ju\n", timestamp, BLOCK_ADD,
            *machine_id, block_id, block_size);
  }
}

void TraceGenerator::AddMachine(const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    machine_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t machine_id = GetMachineId(rd);
    CHECK(InsertIfNotPresent(&machine_res_id_to_trace_id_,
                             ResourceIDFromString(rd.uuid()),
                             machine_id));
    fprintf(machine_events_, "%ju,%ju,%d,,,\n",
            timestamp, machine_id, MACHINE_ADD);
  }
}

void TraceGenerator::AddMachineToRack(ResourceID_t machine_res_id,
                                      uint64_t rack_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* machine_id =
      FindOrNull(machine_res_id_to_trace_id_, machine_res_id);
    CHECK_NOTNULL(machine_id);
    fprintf(machines_to_racks_, "%ju,%d,%ju,%ju\n", timestamp, MACHINE_ADD,
            *machine_id, rack_id);
  }
}

void TraceGenerator::AddTaskInputBlock(const TaskDescriptor& td,
                                       uint64_t block_id) {
  if (FLAGS_generate_trace) {
    uint64_t trace_job_id;
    uint64_t trace_task_id;
    if (td.trace_job_id() != 0) {
      trace_job_id = td.trace_job_id();
      trace_task_id = td.trace_task_id();
    } else {
      trace_job_id = HashString(td.job_id());
      trace_task_id = td.uid();
    }
    fprintf(tasks_to_blocks_, "%ju,%ju,%ju\n",
            trace_job_id, trace_task_id, block_id);
  }
}

void TraceGenerator::AddTaskQuincy(
    const TaskDescriptor& td, uint64_t input_size, int64_t worst_cluster_cost,
    int64_t best_rack_cost, int64_t best_machine_cost,
    int64_t cost_to_unsched, uint64_t num_pref_machines,
    uint64_t num_pref_racks) {
  if (FLAGS_generate_trace && FLAGS_generate_quincy_cost_model_trace) {
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t trace_job_id;
    uint64_t trace_task_id;
    if (td.trace_job_id() != 0) {
      trace_job_id = td.trace_job_id();
      trace_task_id = td.trace_task_id();
    } else {
      trace_job_id = HashString(td.job_id());
      trace_task_id = td.uid();
    }
    fprintf(quincy_tasks_, "%ju,%ju,%ju,%ju,%jd,%jd,%jd,%jd,%ju,%ju\n",
            timestamp, trace_job_id, trace_task_id, input_size,
            worst_cluster_cost, best_rack_cost, best_machine_cost,
            cost_to_unsched, num_pref_machines, num_pref_racks);
  }
}

uint64_t TraceGenerator::GetMachineId(const ResourceDescriptor& rd) {
  if (rd.trace_machine_id() != 0) {
    return rd.trace_machine_id();
  } else {
    return HashString(rd.uuid());
  }
}

void TraceGenerator::RemoveBlock(ResourceID_t machine_res_id,
                                 uint64_t block_id, uint64_t block_size) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* machine_id =
      FindOrNull(machine_res_id_to_trace_id_, machine_res_id);
    CHECK_NOTNULL(machine_id);
    fprintf(dfs_events_, "%ju,%d,%ju,%ju,%ju\n", timestamp, BLOCK_REMOVE,
            *machine_id, block_id, block_size);
  }
}

void TraceGenerator::RemoveMachine(const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    machine_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t machine_id = GetMachineId(rd);
    machine_res_id_to_trace_id_.erase(ResourceIDFromString(rd.uuid()));
    fprintf(machine_events_, "%ju,%ju,%d,,,\n",
            timestamp, machine_id, MACHINE_REMOVE);
  }
}

void TraceGenerator::RemoveMachineFromRack(ResourceID_t machine_res_id,
                                           uint64_t rack_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* machine_id =
      FindOrNull(machine_res_id_to_trace_id_, machine_res_id);
    CHECK_NOTNULL(machine_id);
    fprintf(machines_to_racks_, "%ju,%d,%ju,%ju\n", timestamp, MACHINE_REMOVE,
            *machine_id, rack_id);
  }
}

void TraceGenerator::SchedulerRun(
    const scheduler::SchedulerStats& scheduler_stats,
    const DIMACSChangeStats& dimacs_stats) {
  if (FLAGS_generate_trace) {
    double unscheduled_tasks_percentage = unscheduled_tasks_cnt_ * 100.0 /
      (unscheduled_tasks_cnt_ + running_tasks_cnt_);
    if (unscheduled_tasks_percentage > UNSCHEDULED_TASKS_WARNING_THRESHOLD) {
      LOG(WARNING) << unscheduled_tasks_percentage
                   << "% of tasks are unscheduled";
    }
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    fprintf(scheduler_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju,%ju,%ju,%ju,%s\n",
            timestamp, scheduler_stats.scheduler_runtime_,
            scheduler_stats.algorithm_runtime_,
            scheduler_stats.total_runtime_,
            unscheduled_tasks_cnt_, evicted_tasks_cnt_, migrated_tasks_cnt_,
            unscheduled_tasks_cnt_ + running_tasks_cnt_,
            task_events_cnt_per_round_, machine_events_cnt_per_round_,
            dimacs_stats.GetStatsString().c_str());
    evicted_tasks_cnt_ = 0;
    migrated_tasks_cnt_ = 0;
    task_events_cnt_per_round_ = 0;
    machine_events_cnt_per_round_ = 0;
    fflush(scheduler_events_);
  }
}

void TraceGenerator::TaskSubmitted(TaskDescriptor* td_ptr) {
  if (FLAGS_generate_trace) {
    unscheduled_tasks_cnt_++;
    task_events_cnt_per_round_++;
    // NOTE: We do not use time_manager_ because the task already has a
    // submit time which was set by the coordinator or the simulator.
    uint64_t timestamp = td_ptr->submit_time();
    uint64_t job_id;
    string simulator_job_prefix = "firmament_simulation_job_";
    TaskID_t task_id = td_ptr->uid();
    uint64_t trace_task_id;
    if (td_ptr->trace_job_id() != 0) {
      job_id = td_ptr->trace_job_id();
      trace_task_id = td_ptr->trace_task_id();
    } else {
      job_id = HashString(td_ptr->job_id());
      // Not running in simulation mode => set the id to Firmament task id.
      trace_task_id = task_id;
    }
    // We use the Firmament task id here because the other methods in this
    // class need to access the collection. They only get called with the
    // Firmament task id.
    bool inserted = InsertIfNotPresent(&task_to_job_, task_id, job_id);
    if (inserted) {
      uint64_t* num_tasks = FindOrNull(job_num_tasks_, job_id);
      if (num_tasks == NULL) {
        InsertIfNotPresent(&job_num_tasks_, job_id, 1);
      } else {
        *num_tasks = *num_tasks + 1;
      }
    }
    fprintf(task_events_, "%ju,,%ju,%ju,,%d,,,,,,,\n",
            timestamp, job_id, trace_task_id, TASK_SUBMIT_EVENT);
    fflush(task_events_);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    if (tr_ptr == NULL) {
      TaskRuntime task_runtime;
      task_runtime.task_id_ = trace_task_id;
      task_runtime.start_time_ = timestamp;
      task_runtime.num_runs_ = 0;
      task_runtime.last_schedule_time_ = 0;
      InsertIfNotPresent(&task_to_runtime_, task_id, task_runtime);
    }
  }
}

void TraceGenerator::TaskCompleted(TaskID_t task_id,
                                   const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    running_tasks_cnt_--;
    task_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_FINISH_EVENT);
    fflush(task_events_);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    tr_ptr->runtime_ = timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
    task_to_job_.erase(task_id);
    task_to_runtime_.erase(task_id);
  }
}

void TraceGenerator::TaskEvicted(TaskID_t task_id,
                                 const ResourceDescriptor& rd,
                                 bool migrated) {
  if (FLAGS_generate_trace) {
    running_tasks_cnt_--;
    task_events_cnt_per_round_++;
    if (!migrated) {
      evicted_tasks_cnt_++;
    }
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_EVICT_EVENT);
    fflush(task_events_);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
  }
}

void TraceGenerator::TaskFailed(TaskID_t task_id,
                                const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    running_tasks_cnt_--;
    task_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_FAIL_EVENT);
    fflush(task_events_);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
    task_to_job_.erase(task_id);
    task_to_runtime_.erase(task_id);
  }
}

void TraceGenerator::TaskKilled(TaskID_t task_id,
                                const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    running_tasks_cnt_--;
    task_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_KILL_EVENT);
    fflush(task_events_);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
    task_to_job_.erase(task_id);
    task_to_runtime_.erase(task_id);
  }
}

void TraceGenerator::TaskMigrated(TaskDescriptor* td_ptr,
                                  const ResourceDescriptor& old_rd,
                                  const ResourceDescriptor& new_rd) {
  if (FLAGS_generate_trace) {
    migrated_tasks_cnt_++;
    // Note: We don't have to update the counters here because they're
    // updated in TaskEvicted/Submitted/Scheduled.
    TaskEvicted(td_ptr->uid(), old_rd, true);
    TaskSubmitted(td_ptr);
    TaskScheduled(td_ptr->uid(), new_rd);
  }
}

void TraceGenerator::TaskRemoved(TaskID_t task_id, bool was_running) {
  if (FLAGS_generate_trace) {
    if (was_running) {
      running_tasks_cnt_--;
    }
    task_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,,,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, TASK_REMOVED_EVENT);
    fflush(task_events_);
    task_to_job_.erase(task_id);
    task_to_runtime_.erase(task_id);
  }
}

void TraceGenerator::TaskScheduled(TaskID_t task_id,
                                   const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    running_tasks_cnt_++;
    unscheduled_tasks_cnt_--;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_SCHEDULE_EVENT);
    fflush(task_events_);
    tr_ptr->num_runs_++;
    tr_ptr->last_schedule_time_ = timestamp;
  }
}

} // namespace firmament
