// TODO: header

#include "sim/job_sim.h"

namespace firmament {

JobSim::JobSim(const string& name, uint64_t num_tasks, uint32_t job_type)
  : Job(name),
    start_time_(-numeric_limits<double>::max()),
    finish_time_(-numeric_limits<double>::max()),
    type_(job_type) {
  VLOG(1) << "Creating sim job " << name;
}

bool JobSim::AddTask(TaskSim *const t) {
  tasks_.push_back(t);
  Job::AddTask(t);
  return true;
}

}  // namespace firmament
