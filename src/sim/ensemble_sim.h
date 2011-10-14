// TODO: header

#ifndef FIRMAMENT_SIM_ENSEMBLE_SIM_H
#define FIRMAMENT_SIM_ENSEMBLE_SIM_H

#include "base/common.h"
#include "base/ensemble.h"
#include "base/resource.h"
#include "sim/scheduler_sim.h"
#include "sim/event_queue.h"

namespace firmament {

class SchedulerSim;

class EnsembleSim : public Ensemble {
 public:
  EnsembleSim(const string& name, EventQueue *event_queue);
  void Join(ResourceSim *res);
  void SubmitJob(JobSim *job, double time);
  void RunScheduler();
  uint64_t NumPending();
 private:
  vector<Ensemble*> peered_ensembles_;  // TODO: we may need more detail here
  SchedulerSim *scheduler_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_ENSEMBLE_SIM_H
