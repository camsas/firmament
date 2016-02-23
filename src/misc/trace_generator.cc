// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
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

namespace firmament {

TraceGenerator::TraceGenerator(TimeInterface* time_manager)
  : time_manager_(time_manager), unscheduled_tasks_cnt_(0),
    running_tasks_cnt_(0), evicted_tasks_cnt_(0), task_events_cnt_per_round_(0),
    machine_events_cnt_per_round_(0) {
  if (FLAGS_generate_trace) {
    MkdirIfNotPresent(FLAGS_generated_trace_path);
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/machine_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/scheduler_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_runtime_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/jobs_num_tasks");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_usage_stat");
    string path =
      FLAGS_generated_trace_path + "machine_events/part-00000-of-00001.csv";
    machine_events_ = fopen(path.c_str(), "w");
    CHECK(machine_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "scheduler_events/scheduler_events.csv";
    scheduler_events_ = fopen(path.c_str(), "w");
    CHECK(scheduler_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "task_events/part-00000-of-00500.csv";
    task_events_ = fopen(path.c_str(), "w");
    CHECK(task_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path +
      "task_runtime_events/task_runtime_events.csv";
    task_runtime_events_ = fopen(path.c_str(), "w");
    CHECK(task_runtime_events_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "jobs_num_tasks/jobs_num_tasks.csv";
    jobs_num_tasks_ = fopen(path.c_str(), "w");
    CHECK(jobs_num_tasks_ != NULL) << "Failed to open: " << path;
    path = FLAGS_generated_trace_path + "task_usage_stat/task_usage_stat.csv";
    task_usage_stat_ = fopen(path.c_str(), "w");
    CHECK(task_usage_stat_ != NULL) << "Failed to open: " << path;
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
  }
  // time_manager is not owned by this class. We don't have to delete it here.
}

void TraceGenerator::AddMachine(const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    machine_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t machine_id = GetMachineId(rd);
    fprintf(machine_events_, "%ju,%ju,%d,,,\n",
            timestamp, machine_id, MACHINE_ADD);
  }
}

uint64_t TraceGenerator::GetMachineId(const ResourceDescriptor& rd) {
  if (rd.has_trace_machine_id()) {
    return rd.trace_machine_id();
  } else {
    return HashString(rd.uuid());
  }
}

void TraceGenerator::RemoveMachine(const ResourceDescriptor& rd) {
  if (FLAGS_generate_trace) {
    machine_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t machine_id = GetMachineId(rd);
    fprintf(machine_events_, "%ju,%ju,%d,,,\n",
            timestamp, machine_id, MACHINE_REMOVE);
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
                   << " of tasks are unscheduled";
    }
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    fprintf(scheduler_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju,%s,%ju,%ju\n",
            timestamp, scheduler_stats.scheduler_runtime,
            scheduler_stats.algorithm_runtime, scheduler_stats.total_runtime,
            unscheduled_tasks_cnt_, evicted_tasks_cnt_,
            unscheduled_tasks_cnt_ + running_tasks_cnt_,
            dimacs_stats.GetStatsString().c_str(),
            task_events_cnt_per_round, machine_events_cnt_per_round);
    evicted_tasks_cnt_ = 0;
    task_events_cnt_per_round_ = 0;
    machine_events_cnt_per_round_ = 0;
  }
}

void TraceGenerator::TaskSubmitted(TaskDescriptor* td_ptr) {
  if (FLAGS_generate_trace) {
    unscheduled_tasks_cnt_++;
    task_events_cnt_per_round_++;
    uint64_t timestamp = time_manager_->GetCurrentTimestamp();
    uint64_t job_id;
    string simulator_job_prefix = "firmament_simulation_job_";
    TaskID_t task_id = td_ptr->uid();
    uint64_t trace_task_id;
    if (td_ptr->has_trace_job_id()) {
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
    task_to_job_.erase(task_id);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_FINISH_EVENT);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    tr_ptr->runtime_ = timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
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
    task_to_job_.erase(task_id);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_FAIL_EVENT);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
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
    task_to_job_.erase(task_id);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    uint64_t machine_id = GetMachineId(rd);
    fprintf(task_events_, "%ju,,%ju,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, tr_ptr->task_id_, machine_id,
            TASK_KILL_EVENT);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime_ += timestamp - tr_ptr->last_schedule_time_;
    fprintf(task_runtime_events_, "%ju,%ju,%ju,%ju,%ju,%ju,%ju\n",
            *job_id_ptr, tr_ptr->task_id_, *job_id_ptr, tr_ptr->start_time_,
            tr_ptr->total_runtime_, tr_ptr->runtime_, tr_ptr->num_runs_);
    task_to_runtime_.erase(task_id);
  }
}

void TraceGenerator::TaskMigrated(TaskDescriptor* td_ptr,
                                  const ResourceDescriptor& old_rd,
                                  const ResourceDescriptor& new_rd) {
  if (FLAGS_generate_trace) {
    // Note: We don't have to update the counters here because they're
    // updated in TaskEvicted/Submitted/Scheduled.
    TaskEvicted(td_ptr->uid(), old_rd, true);
    TaskSubmitted(td_ptr);
    TaskScheduled(td_ptr->uid(), new_rd);
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
    tr_ptr->num_runs_++;
    tr_ptr->last_schedule_time_ = timestamp;
  }
}

} // namespace firmament
