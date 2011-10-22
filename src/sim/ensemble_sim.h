// TODO: header

#ifndef FIRMAMENT_SIM_ENSEMBLE_SIM_H
#define FIRMAMENT_SIM_ENSEMBLE_SIM_H

#include "base/common.h"
#include "sim/sim_common.h"
#include "base/ensemble.h"
#include "sim/resource_sim.h"
#include "sim/job_sim.h"
//#include "sim/scheduler_sim.h"

#include <set>

namespace firmament {

class SchedulerSim;
class EventQueue;

class EnsembleSim : public Ensemble {
 public:
  EnsembleSim(const string& name);
  ~EnsembleSim();
  void Join(ResourceSim *res);
  void SubmitJob(JobSim *job, double time);
  void RunScheduler(double time);
  uint64_t NumPendingJobs();
  uint64_t NumPendingTasks();
  void AddPreferredJobType(uint32_t jobtype) {
    preferred_jobtypes_.insert(jobtype);
  }
  set<uint32_t> *preferred_jobtypes() { return &preferred_jobtypes_; }
 private:
  SchedulerSim *scheduler_;
  set<uint32_t> preferred_jobtypes_;
};


}  // namespace firmament

#endif  // FIRMAMENT_SIM_ENSEMBLE_SIM_H
