// TODO: header

#ifndef FIRMAMENT_BASE_ENSEMBLE_H
#define FIRMAMENT_BASE_ENSEMBLE_H

#include "base/common.h"
#include "base/resource.h"

#include <map>

namespace firmament {

class Resource;

class Ensemble {
 public:
  Ensemble(const string& name);
  ~Ensemble();
  bool AddJob(Job *const job);
  void AddResource(Resource& resource);
  void AddTask(Task& task);
  bool AddNestedEnsemble(Ensemble *ensemble);
  vector<Ensemble*> *GetNestedEnsembles() { return &children_; }
  vector<Resource*> *GetResources() { return &joined_resources_; }
  uint64_t NumResourcesJoinedDirectly();
  uint64_t NumNestedEnsembles();
  uint64_t NumIdleResources();
  void SetResourceBusy(Resource *res);
  void SetResourceIdle(Resource *res);
  bool ResourceBusy(Resource *res) { return resources_busy_[res]; }
  string& name() { return name_; }
 protected:
  vector<Resource*> joined_resources_;
  map<Resource*, bool> resources_busy_;
  vector<Ensemble*> children_;
  string name_;

  static const uint64_t nested_ensemble_capacity_ = 64;
  uint64_t num_idle_resources_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_ENSEMBLE_H
