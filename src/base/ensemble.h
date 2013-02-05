// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common ensemble functionality.
// TODO(malte): Refactor this to become more shallow and introduce a separate
//              interface class.

#ifndef FIRMAMENT_BASE_ENSEMBLE_H
#define FIRMAMENT_BASE_ENSEMBLE_H

#include <map>
#include <string>
#include <google/protobuf/repeated_field.h>

#include "base/common.h"
#include "base/resource.h"

#include "base/ensemble_desc.pb.h"

namespace firmament {

using google::protobuf::RepeatedPtrField;

// Forward-declaration to avoid cyclic dependency.
class Resource;

class Ensemble {
 public:
  explicit Ensemble(const string& name);
  ~Ensemble();
  bool AddJob(Job *const job);
  void AddResource(const ResourceDescriptor& resource);
  void AddTask(const Task& task);
  bool AddNestedEnsemble(EnsembleDescriptor *ensemble);
  bool AddPeeredEnsemble(EnsembleDescriptor *ensemble);
  RepeatedPtrField<EnsembleDescriptor> *GetNestedEnsembles() {
    return descriptor_.mutable_nested_ensembles();
  }
  RepeatedPtrField<EnsembleDescriptor> *GetPeeredEnsembles() {
    return descriptor_.mutable_peered_ensembles();
  }
  RepeatedPtrField<ResourceDescriptor> *GetResources() {
    return descriptor_.mutable_joined_resources();
  }
  uint64_t NumResourcesJoinedDirectly();
  uint64_t NumNestedEnsembles();
  uint64_t NumIdleResources(bool include_peers);
  void SetResourceBusy(ResourceDescriptor *res);
  void SetResourceIdle(ResourceDescriptor *res);
  const string& name() { return descriptor_.name(); }
  void set_name(const string& name) {
    descriptor_.set_name(name);
  }

 protected:
  EnsembleDescriptor descriptor_;
  static const uint64_t nested_ensemble_capacity_ = 64;
  uint64_t num_idle_resources_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_ENSEMBLE_H
