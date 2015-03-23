// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Generate Google style trace.

#include <boost/functional/hash.hpp>

#include "base/common.h"
#include "misc/generate_trace.h"
#include "misc/map-util.h"
#include "misc/utils.h"

DEFINE_bool(generate_trace, false, "Generate Google style trace");
DEFINE_string(generated_trace_path, "",
              "Path to where the trace will be generated");

namespace firmament {

GenerateTrace::GenerateTrace() {
  if (FLAGS_generate_trace) {
    string path = FLAGS_generated_trace_path + "machine_events/part-00000-of-00001.csv";
    machine_events_ = fopen(path.c_str(), "w");
    path = FLAGS_generated_trace_path + "task_events/part-00000-of-00500.csv";
    task_events_ = fopen(path.c_str(), "w");
    path = FLAGS_generated_trace_path + "task_runtime_events/task_runtime_events.csv";
    task_runtime_events_ = fopen(path.c_str(), "w");
    path = FLAGS_generated_trace_path + "jobs_num_tasks/jobs_num_tasks.csv";
    jobs_num_tasks_ = fopen(path.c_str(), "w");
    path = FLAGS_generated_trace_path + "task_usage_stat/task_usage_stat.csv";
    task_usage_stat_ = fopen(path.c_str(), "w");
  }
}

GenerateTrace::~GenerateTrace() {
  if (FLAGS_generate_trace) {
    fclose(machine_events_);
    fclose(task_events_);
    // TODO(ionel): Write task runtime events.
    fclose(task_runtime_events_);
    for (unordered_map<uint64_t, uint64_t>::iterator
           it = job_num_tasks_.begin();
         it != job_num_tasks_.end();
         ++it) {
      fprintf(jobs_num_tasks_, "%" PRId64 " %" PRId64 "\n",
              it->first, it->second);
    }
    fclose(jobs_num_tasks_);
    // TODO(ionel): Write task usage stats.
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
    fprintf(machine_events_, "%" PRId64 ",%" PRId64 ",%d,,,\n",
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
    fprintf(machine_events_, "%" PRId64 ",%" PRId64 ",%d,,,\n",
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
        ++num_tasks;
        task_index = *num_tasks;
      }
      InsertIfNotPresent(&task_to_index_, task_id, task_index);
    } else {
      uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
      task_index = *task_index_ptr;
    }
    fprintf(task_events_, "%" PRId64 ",,%" PRId64 ",%" PRId64 ",%d,,,,,,,\n",
            timestamp, job_id_hash, task_index, task_event);
  }
}

void GenerateTrace::TaskCompleted(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 4;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    fprintf(task_events_, "%" PRId64 ",,%" PRId64 ",%" PRId64 ",%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
  }
}

void GenerateTrace::TaskFailed(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 3;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    fprintf(task_events_, "%" PRId64 ",,%" PRId64 ",%" PRId64 ",%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
  }
}

void GenerateTrace::TaskKilled(TaskID_t task_id) {
  if (FLAGS_generate_trace) {
    uint64_t timestamp = GetCurrentTimestamp();
    int32_t task_event = 5;
    uint64_t* job_id_ptr = FindOrNull(task_to_job_, task_id);
    uint64_t* task_index_ptr = FindOrNull(task_to_index_, task_id);
    fprintf(task_events_, "%" PRId64 ",,%" PRId64 ",%" PRId64 ",%d,,,,,,,\n",
            timestamp, *job_id_ptr, *task_index_ptr, task_event);
  }
}

} // namespace firmament
