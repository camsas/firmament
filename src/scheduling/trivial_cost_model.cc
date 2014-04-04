// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#include <string>

#include "scheduling/trivial_cost_model.h"

namespace firmament {

TrivialCostModel::TrivialCostModel() { }

Cost_t TrivialCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 5ULL;
}

Cost_t TrivialCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 2ULL;
}

Cost_t TrivialCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0ULL;
}

Cost_t TrivialCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0ULL;
}

Cost_t TrivialCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

}  // namespace firmament
