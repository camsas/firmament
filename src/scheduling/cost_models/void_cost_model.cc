// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/cost_models/void_cost_model.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"

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
                                                 EquivClass_t tec) {
  return 0LL;
}

Cost_t VoidCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                               ResourceID_t res_id) {
  return 0LL;
}

Cost_t VoidCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                             EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* VoidCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  equiv_classes->push_back(task_id);
  return equiv_classes;
}

vector<EquivClass_t>* VoidCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  LOG(FATAL) << "Not implemented";
  return NULL;
}

vector<ResourceID_t>* VoidCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<TaskID_t>* VoidCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* VoidCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    VoidCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return pair<vector<EquivClass_t>*, vector<EquivClass_t>*>(NULL, NULL);
}

void VoidCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void VoidCostModel::AddTask(TaskID_t task_id) {
}

void VoidCostModel::RemoveMachine(ResourceID_t res_id) {
}

void VoidCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* VoidCostModel::GatherStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* VoidCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  return NULL;
}

} // namespace firmament
