// TODO: header

#include <glog/logging.h>

#include "base/ensemble.h"

namespace firmament {

Ensemble::Ensemble(const string& name) :
  name_(name), num_idle_resources_(0) {
  LOG(INFO) << "Ensemble \"" << name_ << "\" constructed.";
}

Ensemble::~Ensemble() {
  joined_resources_.clear();
  children_.clear();
  // TODO: check if we still have running tasks
  LOG(INFO) << "Ensemble \"" << name_ << "\" destroyed.";
}

void Ensemble::AddResource(Resource& resource) {
  CHECK_GE(joined_resources_.size(), 0);
  VLOG(1) << "Adding resource " << resource.name() << " at " << &resource;
  joined_resources_.push_back(&resource);
  ++num_idle_resources_;
}

void Ensemble::AddTask(Task& task) {
  // TODO: Schedule the task
  CHECK(false);
}

bool Ensemble::AddJob(Job *const job) {
  // TODO:
  return true;
}

bool Ensemble::AddNestedEnsemble(Ensemble *ensemble) {
  // Adds a child ensemble
  if (children_.size() < nested_ensemble_capacity_) {
    children_.push_back(ensemble);
    return true;
  } else {
    LOG(INFO) << "Failed to add nested ensemble: capacity ("
              << nested_ensemble_capacity_ << ") exceeded!";
    return false;
  }
}

bool Ensemble::AddPeeredEnsemble(Ensemble *ensemble) {
  // Adds a peered ensemble
  // TODO: this is obviously a massive simplification of the actual peering
  // process.
  peered_ensembles_.push_back(ensemble);
  return true;
}

uint64_t Ensemble::NumResourcesJoinedDirectly() {
  return joined_resources_.size();
}

uint64_t Ensemble::NumNestedEnsembles() {
  return children_.size();
}

uint64_t Ensemble::NumIdleResources(bool include_peers) {
  // Get idle resource numbers for nested ensembles.i
  uint64_t num_nested_idle = 0;
  uint64_t num_peered_idle = 0;
  for (vector<Ensemble*>::const_iterator c_iter = children_.begin();
       c_iter != children_.end();
       ++c_iter)
    num_nested_idle += (*c_iter)->NumIdleResources(false);

  if (include_peers) {
    for (vector<Ensemble*>::const_iterator p_iter = peered_ensembles_.begin();
         p_iter != peered_ensembles_.end();
         ++p_iter)
      num_peered_idle += (*p_iter)->NumIdleResources(false);
  }

  return num_idle_resources_ + num_nested_idle + num_peered_idle;
}

void Ensemble::SetResourceBusy(Resource *res) {
//  CHECK_LE(idle_resources_.size(), joined_resources_.size());
  res->set_busy(true);
  --num_idle_resources_;
}

void Ensemble::SetResourceIdle(Resource *res) {
//  CHECK_LT(idle_resources_.size(), joined_resources_.size());
  res->set_busy(false);
  ++num_idle_resources_;
}

}  // namespace firmament
