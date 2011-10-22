// TODO: header

#include "sim/ensemble_sim.h"
#include "sim/scheduler_sim.h"

namespace firmament {

EnsembleSim::EnsembleSim(const string& name)
    : Ensemble(name), scheduler_(new SchedulerSim(this)) {

}

EnsembleSim::~EnsembleSim() {
  delete scheduler_;
}

void EnsembleSim::Join(ResourceSim* res) {
  AddResource(*res);
}

void EnsembleSim::SubmitJob(JobSim* job, double time) {
  CHECK_NOTNULL(job);
  scheduler_->SubmitJob(job, time);
}

void EnsembleSim::RunScheduler(double time) {
  scheduler_->ScheduleAllPending(time);
}

uint64_t EnsembleSim::NumPendingJobs() {
  CHECK_NOTNULL(scheduler_);
  return scheduler_->NumPendingJobs();
}

uint64_t EnsembleSim::NumPendingTasks() {
  CHECK_NOTNULL(scheduler_);
  return scheduler_->NumPendingTasks();
}

}  // namespace firmament
