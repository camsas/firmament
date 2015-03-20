// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#include <string>
#include <vector>

#include "misc/map-util.h"
#include "scheduling/trivial_cost_model.h"

namespace firmament {

TrivialCostModel::TrivialCostModel(
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids)
  : task_map_(task_map), leaf_res_ids_(leaf_res_ids) {
}

Cost_t TrivialCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 5LL;
}

Cost_t TrivialCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t TrivialCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 2LL;
}

Cost_t TrivialCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                                ResourceID_t resource_id) {
  return 0LL;
}

Cost_t TrivialCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0LL;
}

Cost_t TrivialCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0LL;
}

Cost_t TrivialCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t TrivialCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                    TaskEquivClass_t tec) {
  return 0ULL;
}

Cost_t TrivialCostModel::EquivClassToResourceNode(TaskEquivClass_t tec,
                                                  ResourceID_t res_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::EquivClassToEquivClass(TaskEquivClass_t tec1,
                                                TaskEquivClass_t tec2) {
  return 0LL;
}

vector<TaskEquivClass_t>* TrivialCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<TaskEquivClass_t>* equiv_classes = new vector<TaskEquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<TaskEquivClass_t>(hash));
  return equiv_classes;
}

vector<ResourceID_t>* TrivialCostModel::GetEquivClassPreferenceArcs(
    TaskEquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  // TODO(ionel): Improve logic to decide how many preference arcs to add.
  uint32_t num_pref_arcs = 1;
  CHECK_GE(leaf_res_ids_->size(),  num_pref_arcs);
  uint32_t rand_seed_ = 0;
  for (uint32_t num_arc = 0; num_arc < num_pref_arcs; ++num_arc) {
    size_t index = rand_r(&rand_seed_) % leaf_res_ids_->size();
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>::iterator it =
      leaf_res_ids_->begin();
    advance(it, index);
    prefered_res->push_back(*it);
  }
  return prefered_res;
}

vector<ResourceID_t>* TrivialCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

pair<vector<ResourceID_t>*, vector<ResourceID_t>*>
  TrivialCostModel::GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return pair<vector<ResourceID_t>*, vector<ResourceID_t>*>(NULL, NULL);
}

}  // namespace firmament
