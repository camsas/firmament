// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#include <set>
#include <string>

#include "scheduling/trivial_cost_model.h"

namespace firmament {

TrivialCostModel::TrivialCostModel() { }

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

Cost_t TrivialCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::EquivClassToResourceNode(TaskID_t task_id,
                                                  ResourceID_t res_id) {
  return 0ULL;
}

set<TaskEquivClass_t>* TrivialCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  return NULL;
}

set<ResourceID_t>* TrivialCostModel::GetEquivClassPreferenceArcs(
    TaskEquivClass_t tec) {
  return NULL;
}

set<ResourceID_t>* TrivialCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  return NULL;
}

}  // namespace firmament
