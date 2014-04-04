// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include <string>

#include "scheduling/quincy_cost_model.h"

namespace firmament {

QuincyCostModel::QuincyCostModel() { }

Cost_t QuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 5ULL;
}

Cost_t QuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 2ULL;
}

Cost_t QuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0ULL;
}

Cost_t QuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0ULL;
}

Cost_t QuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

}  // namespace firmament
