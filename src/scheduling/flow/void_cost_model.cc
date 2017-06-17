/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include "scheduling/flow/void_cost_model.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

VoidCostModel::VoidCostModel(shared_ptr<ResourceMap_t> resource_map,
                             shared_ptr<TaskMap_t> task_map)
    : resource_map_(resource_map), task_map_(task_map) {
}

ArcDescriptor VoidCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor VoidCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

Cost_t VoidCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 0LL;
}

ArcDescriptor VoidCostModel::TaskToResourceNode(TaskID_t task_id,
                                                ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

Cost_t VoidCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0LL;
}

ArcDescriptor VoidCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor VoidCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor VoidCostModel::TaskContinuation(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor VoidCostModel::TaskPreemption(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor VoidCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                        EquivClass_t tec) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor VoidCostModel::EquivClassToResourceNode(
    EquivClass_t tec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  return ArcDescriptor(0LL, num_free_slots, 0ULL);
}

ArcDescriptor VoidCostModel::EquivClassToEquivClass(
    EquivClass_t tec1,
    EquivClass_t tec2) {
  return ArcDescriptor(0LL, 0ULL, 0ULL);
}

vector<EquivClass_t>* VoidCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // We have one task EC per program.
  // The ID of the aggregator is the hash of the command line.
  // We need this EC even in the void cost model in order to make the EC
  // statistics view on the web UI work.
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashCommandLine(*td_ptr));
  equiv_classes->push_back(task_agg);
  equiv_classes->push_back(task_id);
  return equiv_classes;
}

vector<ResourceID_t>* VoidCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  for (auto& res_id_rtnd : machine_to_rtnd_) {
    prefered_res->push_back(res_id_rtnd.first);
  }
  return prefered_res;
}

vector<ResourceID_t>* VoidCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  return NULL;
}

vector<EquivClass_t>* VoidCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t tec) {
  return NULL;
}

void VoidCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_,
                     ResourceIDFromString(rtnd_ptr->resource_desc().uuid()),
                     rtnd_ptr);
}

void VoidCostModel::AddTask(TaskID_t task_id) {
}

void VoidCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machine_to_rtnd_.erase(res_id), 1);
}

void VoidCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* VoidCostModel::GatherStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    // The other node is not a resource node.
    if (other->type_ == FlowNodeType::SINK) {
      accumulator->rd_ptr_->set_num_running_tasks_below(
          static_cast<uint64_t>(
              accumulator->rd_ptr_->current_running_tasks_size()));
      accumulator->rd_ptr_->set_num_slots_below(FLAGS_max_tasks_per_pu);
    }
    return accumulator;
  }

  CHECK_NOTNULL(other->rd_ptr_);
  accumulator->rd_ptr_->set_num_running_tasks_below(
      accumulator->rd_ptr_->num_running_tasks_below() +
      other->rd_ptr_->num_running_tasks_below());
  accumulator->rd_ptr_->set_num_slots_below(
      accumulator->rd_ptr_->num_slots_below() +
      other->rd_ptr_->num_slots_below());
  return accumulator;
}

void VoidCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* VoidCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  return accumulator;
}

} // namespace firmament
