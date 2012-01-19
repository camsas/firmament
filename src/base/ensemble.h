// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common ensemble functionality.
// TODO(malte): Refactor this to become more shallow and introduce a separate
//              interface class.

#ifndef FIRMAMENT_BASE_ENSEMBLE_H
#define FIRMAMENT_BASE_ENSEMBLE_H

#include <map>

#include "base/common.h"
#include "base/resource.h"

#include "base/ensemble_desc.pb.h"

namespace firmament {

// Forward-declaration to avoid cyclic dependency.
class Resource;

class Ensemble {
 public:
  Ensemble(const string& name);
  ~Ensemble();
  bool AddJob(Job *const job);
  void AddResource(Resource& resource);
  void AddTask(Task& task);
  bool AddNestedEnsemble(Ensemble *ensemble);
  bool AddPeeredEnsemble(Ensemble *ensemble);
  vector<Ensemble*> *GetNestedEnsembles() { return &children_; }
  vector<Ensemble*> *GetPeeredEnsembles() { return &peered_ensembles_; }
  vector<Resource*> *GetResources() { return &joined_resources_; }
  uint64_t NumResourcesJoinedDirectly();
  uint64_t NumNestedEnsembles();
  uint64_t NumIdleResources(bool include_peers);
  void SetResourceBusy(Resource *res);
  void SetResourceIdle(Resource *res);
  string& name() { return name_; }
 protected:
  vector<Resource*> joined_resources_;
  vector<Ensemble*> children_;
  vector<Ensemble*> peered_ensembles_;  // TODO: we may need more detail here
  string name_;

  static const uint64_t nested_ensemble_capacity_ = 64;
  uint64_t num_idle_resources_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_ENSEMBLE_H
