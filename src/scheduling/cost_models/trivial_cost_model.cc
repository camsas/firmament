// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#include <string>
#include <vector>

#include "misc/map-util.h"
#include "scheduling/cost_models/trivial_cost_model.h"

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
                                                    EquivClass_t tec) {
  return 0ULL;
}

Cost_t TrivialCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                                  ResourceID_t res_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                                EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* TrivialCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* TrivialCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  LOG(FATAL) << "Not implemented";
  return NULL;
}

vector<ResourceID_t>* TrivialCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
  uint32_t rand_seed_ = 0;
  for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
       ++num_arc) {
    size_t index = rand_r(&rand_seed_) % leaf_res_ids_->size();
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>::iterator it =
      leaf_res_ids_->begin();
    advance(it, index);
    prefered_res->push_back(*it);
  }
  return prefered_res;
}

vector<TaskID_t>* TrivialCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* TrivialCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    TrivialCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(equiv_classes, equiv_classes);
}

void TrivialCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void TrivialCostModel::AddTask(TaskID_t task_id) {
}

void TrivialCostModel::RemoveMachine(ResourceID_t res_id) {
}

void TrivialCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* TrivialCostModel::GatherStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* TrivialCostModel::UpdateStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
