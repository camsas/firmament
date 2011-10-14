// TODO: header

#ifndef FIRMAMENT_SIM_SCHEDULER_SIM
#define FIRMAMENT_SIM_SCHEDULER_SIM

#include "base/common.h"
#include "sim/job_sim.h"
#include "sim/ensemble_sim.h"
#include "sim/event_queue.h"

#include <queue>
#include <utility>

namespace firmament {

class EnsembleSim;

class SchedulerSim {
 public:
  SchedulerSim(EnsembleSim *ensemble, EventQueue *event_queue);
  void ScheduleAllPending();
  uint64_t ScheduleJob(JobSim *job, double time);
  void SubmitJob(JobSim *job, double time);
  uint64_t NumPending() { return pending_queue_.size(); }
 private:
  queue<pair<JobSim*, double> > pending_queue_;
  EnsembleSim *ensemble_;
  double time_;
  EventQueue *global_event_queue_;
};

}

#endif  // FIRMAMENT_SIM_SCHEDULER_SIM
