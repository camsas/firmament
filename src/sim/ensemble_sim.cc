// TODO: header

#include "sim/ensemble_sim.h"

namespace firmament {

EnsembleSim::EnsembleSim(const string& name, EventQueue *event_queue)
    : Ensemble(name), scheduler_(new SchedulerSim(this, event_queue)) {

}

void EnsembleSim::Join(ResourceSim* res) {
  AddResource(*res);
}

void EnsembleSim::SubmitJob(JobSim* job, double time) {
  scheduler_->SubmitJob(job, time);
}

void EnsembleSim::RunScheduler() {
  scheduler_->ScheduleAllPending();
}

uint64_t EnsembleSim::NumPending() {
  CHECK_NOTNULL(scheduler_);
  return scheduler_->NumPending();
}

}  // namespace firmament
