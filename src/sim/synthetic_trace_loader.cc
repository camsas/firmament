// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/synthetic_trace_loader.h"

#include "base/units.h"

DEFINE_uint64(synthetic_machine_failure_rate, 1,
              "Number of machine failures per hour");
DEFINE_uint64(synthetic_machine_failure_duration, 60,
              "Duration (in seconds) of a machine failure");
DEFINE_uint64(synthetic_num_jobs, 100,
              "Total number of synthetic jobs to generate");
DEFINE_uint64(synthetic_num_machines, 10, "Number of machines to simulate");
DEFINE_uint64(synthetic_job_interarrival_time, 1000000,
              "Number of microseconds in between job arrivals");
DEFINE_uint64(synthetic_tasks_per_job, 2,
              "Number of tasks per job");
DEFINE_uint64(synthetic_task_duration, 10 * firmament::SECONDS_TO_MICROSECONDS,
              "Duration (in microseconds) of a task");

DECLARE_uint64(runtime);

namespace firmament {
namespace sim {

SyntheticTraceLoader::SyntheticTraceLoader(EventManager* event_manager)
  : TraceLoader(event_manager), last_generated_job_id_(0) {
  if (FLAGS_runtime == UINT64_MAX) {
    LOG(FATAL) << "Cannot add machine failure events for a simulation "
               << "without runtime set";
  }
}

void SyntheticTraceLoader::LoadJobsNumTasks(
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  uint64_t usec_between_jobs = FLAGS_synthetic_job_interarrival_time;
  uint64_t job_id = 1;
  for (uint64_t timestamp = 0;
       timestamp <= FLAGS_runtime && job_id < FLAGS_synthetic_num_jobs;
       timestamp += usec_between_jobs, ++job_id) {
    CHECK(InsertIfNotPresent(job_num_tasks, job_id,
                             FLAGS_synthetic_tasks_per_job));
  }
}

void SyntheticTraceLoader::LoadMachineEvents(
    multimap<uint64_t, EventDescriptor>* machine_events) {
  for (uint64_t machine_id = 1; machine_id <= FLAGS_synthetic_num_machines;
       ++machine_id) {
    EventDescriptor event_desc;
    event_desc.set_machine_id(machine_id);
    event_desc.set_type(EventDescriptor::ADD_MACHINE);
    machine_events->insert(pair<uint64_t, EventDescriptor>(0, event_desc));
  }
  uint32_t rand_seed = 0;
  unordered_map<uint64_t, uint64_t> machine_recovery;
  for (uint64_t timestamp = 0;
       timestamp <= FLAGS_runtime;
       timestamp += SECONDS_IN_HOUR * MICROSECONDS_IN_SECOND) {
    for (uint64_t failure_index = 0;
         failure_index < FLAGS_synthetic_machine_failure_rate;
         ++failure_index) {
      uint64_t failure_timestamp =
        timestamp + static_cast<uint64_t>(rand_r(&rand_seed)) %
        (SECONDS_IN_HOUR * MICROSECONDS_IN_SECOND);
      EventDescriptor event_desc;
      uint64_t machine_id = 0;
      // Loop until we find a machine id that is not currently in a failed state
      while (true) {
        machine_id = static_cast<uint64_t>(rand_r(&rand_seed)) %
          FLAGS_synthetic_num_machines + 1;
        uint64_t* recovery_timestamp = FindOrNull(machine_recovery, machine_id);
        if (!recovery_timestamp || *recovery_timestamp < failure_timestamp) {
          break;
        }
      }
      event_desc.set_machine_id(machine_id);
      event_desc.set_type(EventDescriptor::REMOVE_MACHINE);
      machine_events->insert(
          pair<uint64_t, EventDescriptor>(failure_timestamp, event_desc));
      // The failure is temporary. Add the machine back.
      uint64_t recovery_timestamp = failure_timestamp +
        FLAGS_synthetic_machine_failure_duration * SECONDS_TO_MICROSECONDS;
      event_desc.set_type(EventDescriptor::ADD_MACHINE);
      machine_events->insert(
          pair<uint64_t, EventDescriptor>(recovery_timestamp, event_desc));
      InsertOrUpdate(&machine_recovery, machine_id, recovery_timestamp);
    }
  }
}

bool SyntheticTraceLoader::LoadTaskEvents(
    uint64_t events_up_to_time,
    unordered_map<uint64_t, uint64_t>* job_num_tasks) {
  uint64_t usec_between_jobs = FLAGS_synthetic_job_interarrival_time;
  uint64_t current_timestamp = last_generated_job_id_ * usec_between_jobs;
  if (current_timestamp > events_up_to_time) {
    return true;
  }
  for (uint64_t timestamp = current_timestamp;
       last_generated_job_id_ < FLAGS_synthetic_num_jobs;
       timestamp += usec_between_jobs) {
    last_generated_job_id_++;
    for (uint64_t task_index = 1; task_index <= FLAGS_synthetic_tasks_per_job;
         ++task_index) {
      EventDescriptor event_desc;
      event_desc.set_job_id(last_generated_job_id_);
      event_desc.set_task_index(task_index);
      event_desc.set_type(EventDescriptor::TASK_SUBMIT);
      event_manager_->AddEvent(timestamp, event_desc);
    }
    if (timestamp > events_up_to_time) {
      // We want to add one additional event after events_up_to_time to make
      // sure that the simulation doesn't end.
      return true;
    }
  }
  return true;
}

void SyntheticTraceLoader::LoadTaskUtilizationStats(
    unordered_map<TaskID_t, TraceTaskStats>* task_id_to_stats) {
  uint64_t usec_between_jobs = FLAGS_synthetic_job_interarrival_time;
  TraceTaskStats task_stats;
  task_stats.avg_mean_cpu_usage_ = 0.5;
  task_stats.avg_canonical_mem_usage_ = 0.2;
  task_stats.avg_assigned_mem_usage_ = 0.2;
  task_stats.avg_unmapped_page_cache_ = 0.2;
  task_stats.avg_total_page_cache_ = 0.2;
  uint64_t job_id = 1;
  for (uint64_t timestamp = 0;
       timestamp <= FLAGS_runtime && job_id < FLAGS_synthetic_num_jobs;
       timestamp += usec_between_jobs, ++job_id) {
    TraceTaskIdentifier task_identifier;
    task_identifier.job_id = job_id;
    for (uint64_t task_index = 1; task_index <= FLAGS_synthetic_tasks_per_job;
         ++task_index) {
      task_identifier.task_index = task_index;
      CHECK(InsertIfNotPresent(
          task_id_to_stats, GenerateTaskIDFromTraceIdentifier(task_identifier),
          task_stats));
    }
  }
}

void SyntheticTraceLoader::LoadTasksRunningTime(
    unordered_map<TaskID_t, uint64_t>* task_runtime) {
  uint64_t usec_between_jobs = FLAGS_synthetic_job_interarrival_time;
  uint64_t job_id = 1;
  for (uint64_t timestamp = 0;
       timestamp <= FLAGS_runtime && job_id < FLAGS_synthetic_num_jobs;
       timestamp += usec_between_jobs, ++job_id) {
    TraceTaskIdentifier task_identifier;
    task_identifier.job_id = job_id;
    for (uint64_t task_index = 1; task_index <= FLAGS_synthetic_tasks_per_job;
         ++task_index) {
      task_identifier.task_index = task_index;
      CHECK(InsertIfNotPresent(
          task_runtime, GenerateTaskIDFromTraceIdentifier(task_identifier),
          FLAGS_synthetic_task_duration));
    }
  }
}

} // namespace sim
} // namespace firmament
