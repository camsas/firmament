// TODO: header

#include "sim/job_sim.h"

namespace firmament {

JobSim::JobSim(const string& name, uint64_t num_tasks)
  : Job(name),
    start_time_(-numeric_limits<double>::max()),
    finish_time_(-numeric_limits<double>::max()) {
  VLOG(1) << "Creating sim job " << name;
}

bool JobSim::AddTask(TaskSim *const t) {
  tasks_.push_back(t);
  Job::AddTask(static_cast<Task *const>(t));
  return true;
}

}  // namespace firmament
