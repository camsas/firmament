// TODO

#include "sim/workload_generator.h"
#include "sim/sim_common.h"

namespace firmament {

DEFINE_double(simulation_runtime, 10.0, "Total duration of simulation.");
DEFINE_double(job_size_lambda, 0.05, "Lamda for job size distribution.");
DEFINE_double(job_arrival_lambda, 2.0,
              "Lambda for interarrival time distribution.");
DEFINE_double(task_duration_scaling_factor, 30,
              "Task duration distribution scaling factor.");
DEFINE_double(job_arrival_scaling_factor, 1.5,
              "Interarrival distribution scaling factor.");

WorkloadGenerator::WorkloadGenerator() :
  task_duration_distribution_(1.0, 1.0),
  job_arrival_distribution_(FLAGS_job_arrival_lambda),
  job_size_distribution_(FLAGS_job_size_lambda),
  job_type_distribution_(0, 1),
  job_arrival_gen_(gen_, job_arrival_distribution_),
  job_size_gen_(gen_, job_size_distribution_),
  job_type_gen_(gen_, job_type_distribution_),
  task_duration_gen_(gen_, task_duration_distribution_)
{
  LOG(INFO) << "hello from workload generator";
}

double WorkloadGenerator::GetNextInterarrivalTime() {
  return FLAGS_job_arrival_scaling_factor * job_arrival_gen_();
}

uint64_t WorkloadGenerator::GetNextJobSize() {
  return static_cast<uint64_t>(1 + job_size_gen_());
}

uint32_t WorkloadGenerator::GetNextJobType() {
  return job_type_gen_();
}

double WorkloadGenerator::GetNextTaskDuration() {
  return FLAGS_task_duration_scaling_factor * task_duration_gen_();
}

}  // namespace firmament
