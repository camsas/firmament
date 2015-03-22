// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/void_cost_model.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

VoidCostModel::VoidCostModel() {
}

Cost_t VoidCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                             ResourceID_t resource_id) {
  return 0LL;
}

Cost_t VoidCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0LL;
}

Cost_t VoidCostModel::ResourceNodeToResourceNodeCost(ResourceID_t source,
                                                     ResourceID_t destination) {
  return 0LL;
}

Cost_t VoidCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                 TaskEquivClass_t tec) {
  return 0LL;
}

Cost_t VoidCostModel::EquivClassToResourceNode(TaskEquivClass_t tec,
                                               ResourceID_t res_id) {
  return 0LL;
}

Cost_t VoidCostModel::EquivClassToEquivClass(TaskEquivClass_t tec1,
                                             TaskEquivClass_t tec2) {
  return 0LL;
}

vector<TaskEquivClass_t>* VoidCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<TaskEquivClass_t>* equiv_classes = new vector<TaskEquivClass_t>();
  equiv_classes->push_back(task_id);
  return equiv_classes;
}

vector<ResourceID_t>* VoidCostModel::GetEquivClassPreferenceArcs(
    TaskEquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* VoidCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>
    VoidCostModel::GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>(NULL, NULL);
}

} // namespace firmament
