// TODO

#include "workload_generator.h"

namespace firmament {

WorkloadGenerator::WorkloadGenerator() : 
  task_duration_distribution_(1.0, 1.0),
  job_arrival_gen_(gen_, job_arrival_distribution_),
  job_size_gen_(gen_, job_size_distribution_),
  task_duration_gen_(gen_, task_duration_distribution_) 
{
  LOG(INFO) << "hello from workload generator";
}

double WorkloadGenerator::GetNextInterarrivalTime() {
  return job_arrival_gen_();
}

uint64_t WorkloadGenerator::GetNextJobSize() {
  return static_cast<uint64_t>(1 + job_size_gen_());
}

double WorkloadGenerator::GetNextTaskDuration() {
  return task_duration_gen_();
}

}  // namespace firmament
