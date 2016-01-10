// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Generate Google style trace.

#include "misc/generate_trace.h"

#include <string>
#include <boost/functional/hash.hpp>

#include "base/common.h"
#include "misc/map-util.h"
#include "misc/utils.h"

DEFINE_bool(generate_trace, false, "Generate Google style trace");
DEFINE_string(generated_trace_path, "",
              "Path to where the trace will be generated");

namespace firmament {

GenerateTrace::GenerateTrace() {
  if (FLAGS_generate_trace) {
    MkdirIfNotPresent(FLAGS_generated_trace_path);
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/machine_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_runtime_events");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/jobs_num_tasks");
    MkdirIfNotPresent(FLAGS_generated_trace_path + "/task_usage_stat");
    string path =
      FLAGS_generated_trace_path + "machine_events/part-00000-of-00001.csv";
    machine_events_ = fopen(path.c_str(), "w");
    CHECK(machine_events_ != NULL) << "Failed to open: " << path;
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

GenerateTrace::~GenerateTrace() {
  if (FLAGS_generate_trace) {
    fclose(machine_events_);
    fclose(task_events_);
    for (auto& task_id_runtime : task_to_runtime_) {
      uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id_runtime.first);
      uint64_t* task_index_ptr =
        FindOrNull(task_to_index_, task_id_runtime.first);
      TaskRuntime task_runtime = task_id_runtime.second;
      // NOTE: We are using the job id as the job logical name.
      fprintf(task_runtime_events_, "%ju %ju %ju %ju %ju %ju %ju\n",
              *job_id_ptr, *task_index_ptr, *job_id_ptr,
              task_runtime.start_time, task_runtime.total_runtime,
              task_runtime.runtime, task_runtime.num_runs);
    }
    fclose(task_runtime_events_);
    for (auto& job_to_num_tasks : job_num_tasks_) {
      fprintf(jobs_num_tasks_, "%ju %ju\n", job_to_num_tasks.first,
              job_to_num_tasks.second);
    }
    fclose(jobs_num_tasks_);
    // TODO(ionel): Collect task usage stats.
    // for (auto& task_to_job : task_to_job_) {
    //   uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_to_job.first);
    //   uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_to_job.first);
    //   fprintf(task_usage_stat_, "%jd %jd 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 "
    //           "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n", *job_id_ptr,
    //           *task_index_ptr);
    // }
    fclose(task_usage_stat_);
  }
}

void GenerateTrace::AddMachine(ResourceID_t res_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    size_t hash = 42;
    boost::hash_combine(hash, res_id);
    uint64_t machine_id = static_cast<uint64_t>(hash);
    // 0 corresponds to add machine.
    int32_t machine_event = 0;
    fprintf(machine_events_, "%jd %ju %d,,,\n",
            timestamp, machine_id, machine_event);
  }
}

void GenerateTrace::RemoveMachine(ResourceID_t res_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    size_t hash = 42;
    boost::hash_combine(hash, res_id);
    uint64_t machine_id = static_cast<uint64_t>(hash);
    // 0 corresponds to add machine.
    int32_t machine_event = 1;
    fprintf(machine_events_, "%ju,%ju,%d,,,\n",
            timestamp, machine_id, machine_event);
  }
}

void GenerateTrace::TaskSubmitted(JobID_t job_id, TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 0;
    size_t hash = 42;
    boost::hash_combine(hash, job_id);
    uint64_t job_id_hash = static_cast<uint64_t>(hash);
    bool inserted = InsertIfNotPresent(&task_to_job_, task_id, job_id_hash);
    uint64_t task_index = 0;
    if (inserted) {
      uint64_t* num_tasks = FindOrNull(job_num_tasks_, job_id_hash);
      if (num_tasks == NULL) {
        InsertIfNotPresent(&job_num_tasks_, job_id_hash, 1);
        task_index = 1;
      } else {
        *num_tasks = *num_tasks + 1;
        task_index = *num_tasks;
      }
      InsertIfNotPresent(&task_to_index_, task_id, task_index);
    } else {
      uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
      task_index = *task_index_ptr;
    }
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, job_id_hash, task_index, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    if (tr_ptr == NULL) {
      TaskRuntime task_runtime;
      task_runtime.start_time = timestamp;
      task_runtime.num_runs = 0;
      task_runtime.last_schedule_time = 0;
      InsertIfNotPresent(&task_to_runtime_, task_id, task_runtime);
    }
  }
}

void GenerateTrace::TaskCompleted(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 4;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    CHECK_NOTNULL(task_index_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime += timestamp - tr_ptr->last_schedule_time;
    tr_ptr->runtime = timestamp - tr_ptr->last_schedule_time;
  }
}

void GenerateTrace::TaskEvicted(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 2;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    CHECK_NOTNULL(task_index_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime += timestamp - tr_ptr->last_schedule_time;
  }
}

void GenerateTrace::TaskFailed(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 3;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    CHECK_NOTNULL(task_index_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime += timestamp - tr_ptr->last_schedule_time;
  }
}

void GenerateTrace::TaskKilled(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 5;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    CHECK_NOTNULL(task_index_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    // XXX(ionel): This assumes that only one task with task_id is running
    // at a time.
    tr_ptr->total_runtime += timestamp - tr_ptr->last_schedule_time;
  }
}

void GenerateTrace::TaskScheduled(TaskID_t task_id, ResourceID_t res_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 1;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    CHECK_NOTNULL(job_id_ptr);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    CHECK_NOTNULL(task_index_ptr);
    fprintf(task_events_, "%ju,,%ju,%ju,%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
    TaskRuntime* tr_ptr = FindOrNull(task_to_runtime_, task_id);
    CHECK_NOTNULL(tr_ptr);
    tr_ptr->num_runs++;
    tr_ptr->last_schedule_time = timestamp;
  }
}

} // namespace firmament
