// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common ensemble functionality.
// N.B.: Much of the code in this class is subject to change in the near
// future.

#include <glog/logging.h>

#include "base/ensemble.h"

namespace firmament {

Ensemble::Ensemble(const string& name) :
  num_idle_resources_(0) {
  descriptor_.set_name(name);
  LOG(INFO) << "Ensemble \"" << name << "\" constructed.";
}

Ensemble::~Ensemble() {
  // TODO(malte): Use machinery to disassociate resources, rather than just
  // dropping their descriptors!
  descriptor_.clear_joined_resources();
  descriptor_.clear_nested_ensembles();
  // TODO: check if we still have running tasks
  LOG(INFO) << "Ensemble \"" << descriptor_.name() << "\" destroyed.";
  // N.B.: The ensemble descriptor gets deallocated automaticallywith the
  // object.
}

void Ensemble::AddResource(ResourceDescriptor& resource) {
  CHECK_GE(descriptor_.joined_resources_size(), 0);
  VLOG(1) << "Adding resource " << resource.name() << " at " << &resource;
  // TODO(malte): This is somewhat ugly; the protobuf API only allows us to add
  // a new element to a repeated field by first obtaining a pointer and then
  // manipulating it. However, in this case, we already have an ensemble
  // descriptor, so we'd like to insert it directly. Currently, we perform a
  // deep copy.
  ResourceDescriptor* new_resource_descriptor =
      descriptor_.add_joined_resources();
  *new_resource_descriptor = resource;  // copy!
  if (resource.state() == ResourceDescriptor::RESOURCE_IDLE)
    ++num_idle_resources_;
}

void Ensemble::AddTask(Task& task) {
  // TODO: Schedule the task
  CHECK(false);
}

bool Ensemble::AddJob(Job *const job) {
  // TODO: currently a stub
  return true;
}

bool Ensemble::AddNestedEnsemble(EnsembleDescriptor *ensemble) {
  // Adds a child ensemble
  if (descriptor_.nested_ensembles_size() < nested_ensemble_capacity_) {
    EnsembleDescriptor* new_ensemble_desc = descriptor_.add_nested_ensembles();
    *new_ensemble_desc = *ensemble;  // copy!
    return true;
  } else {
    LOG(INFO) << "Failed to add nested ensemble: capacity ("
              << nested_ensemble_capacity_ << ") exceeded!";
    return false;
  }
}

bool Ensemble::AddPeeredEnsemble(EnsembleDescriptor *ensemble) {
  // Adds a peered ensemble
  // TODO: this is obviously a massive simplification of the actual peering
  // process.
  EnsembleDescriptor* new_ensemble_desc = descriptor_.add_peered_ensembles();
  *new_ensemble_desc = *ensemble;  // copy!
  return true;
}

uint64_t Ensemble::NumResourcesJoinedDirectly() {
  return descriptor_.joined_resources_size();
}

uint64_t Ensemble::NumNestedEnsembles() {
  return descriptor_.nested_ensembles_size();
}

uint64_t Ensemble::NumIdleResources(bool include_peers) {
  // Get idle resource numbers for nested ensembles.i
  uint64_t num_nested_idle = 0;
  uint64_t num_peered_idle = 0;
  for (RepeatedPtrField<EnsembleDescriptor>::const_iterator ne_iter =
       descriptor_.nested_ensembles().begin();
       ne_iter != descriptor_.nested_ensembles().end();
       ++ne_iter)
    //num_nested_idle += (*ne_iter)->NumIdleResources(false);

  if (include_peers) {
    for (RepeatedPtrField<EnsembleDescriptor>::const_iterator p_iter =
         descriptor_.peered_ensembles().begin();
         p_iter != descriptor_.peered_ensembles().end();
         ++p_iter)
      num_peered_idle += 0;  // XXX(malte): broken, need to redesign!
      //num_peered_idle += (*p_iter)->NumIdleResources(false);
  }

  return num_idle_resources_ + num_nested_idle + num_peered_idle;
}

void Ensemble::SetResourceBusy(ResourceDescriptor *res) {
//  CHECK_LE(idle_resources_.size(), joined_resources_.size());
  res->set_state(ResourceDescriptor::RESOURCE_BUSY);
  --num_idle_resources_;
}

void Ensemble::SetResourceIdle(ResourceDescriptor *res) {
//  CHECK_LT(idle_resources_.size(), joined_resources_.size());
  res->set_state(ResourceDescriptor::RESOURCE_IDLE);
  ++num_idle_resources_;
}

}  // namespace firmament
