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
  VLOG(1) << "Adding resource " << resource.name();
  joined_resources_.push_back(&resource);
  resources_busy_[&resource] = false;
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

uint64_t Ensemble::NumResourcesJoinedDirectly() {
  return joined_resources_.size();
}

uint64_t Ensemble::NumNestedEnsembles() {
  return children_.size();
}

uint64_t Ensemble::NumIdleResources() {
  return num_idle_resources_;
}

void Ensemble::SetResourceBusy(Resource *res) {
//  CHECK_LE(idle_resources_.size(), joined_resources_.size());
  resources_busy_[res] = true;
  --num_idle_resources_;
}

void Ensemble::SetResourceIdle(Resource *res) {
//  CHECK_LT(idle_resources_.size(), joined_resources_.size());
  resources_busy_[res] = false;
  ++num_idle_resources_;
}

}  // namespace firmament
