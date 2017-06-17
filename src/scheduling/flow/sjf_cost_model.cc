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

// Simple shortest-job-first scheduling cost model.

#include "scheduling/flow/sjf_cost_model.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"

DECLARE_bool(preemption);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

SJFCostModel::SJFCostModel(shared_ptr<ResourceMap_t> resource_map,
                           shared_ptr<TaskMap_t> task_map,
                           unordered_set<ResourceID_t,
                             boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                           shared_ptr<KnowledgeBase> knowledge_base,
                           TimeInterface* time_manager)
  : resource_map_(resource_map),
    knowledge_base_(knowledge_base),
    task_map_(task_map),
    time_manager_(time_manager) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
}

const TaskDescriptor& SJFCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
ArcDescriptor SJFCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t now = time_manager_->GetCurrentTimestamp();
  uint64_t time_since_submit = now - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  uint64_t wait_time_centamillis = time_since_submit / 100000;
  // Cost is the max of the average runtime and the wait time, so that the
  // average runtime is a lower bound on the cost.
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  Cost_t avg_runtime = static_cast<Cost_t>(
      knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front()));
  delete equiv_classes;
  return ArcDescriptor(max(static_cast<Cost_t>(WAIT_TIME_MULTIPLIER *
                                               wait_time_centamillis),
                        avg_runtime * 100),
                    1ULL, 0ULL);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
ArcDescriptor SJFCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t SJFCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  Cost_t avg_runtime = static_cast<Cost_t>(
      knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front()));
  delete equiv_classes;
  return avg_runtime * 100;
}

ArcDescriptor SJFCostModel::TaskToResourceNode(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  return ArcDescriptor(TaskToClusterAggCost(task_id), 1ULL, 0ULL);
}

ArcDescriptor SJFCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

// The cost from the resource leaf to the sink is 0.
ArcDescriptor SJFCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor SJFCostModel::TaskContinuation(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor SJFCostModel::TaskPreemption(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor SJFCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                       EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_)
    return ArcDescriptor(TaskToClusterAggCost(task_id) + 1, 1ULL, 0ULL);
  else
    return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor SJFCostModel::EquivClassToResourceNode(
    EquivClass_t tec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  return ArcDescriptor(0LL, num_free_slots, 0ULL);
}

ArcDescriptor SJFCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                                   EquivClass_t tec2) {
  return ArcDescriptor(0LL, 0ULL, 0ULL);
}

vector<EquivClass_t>* SJFCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  equiv_classes->push_back(
      static_cast<EquivClass_t>(HashString(td_ptr->binary())));
  // All tasks also have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* SJFCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator, and has arcs to all machines.
    // XXX(malte): This is inefficient, as it needlessly adds all the
    // machines every time we call this. To optimize, we can just include
    // the ones for which arcs are missing.
    for (auto it = machine_to_rtnd_.begin();
         it != machine_to_rtnd_.end();
         ++it) {
      prefered_res->push_back(it->first);
    }
  }
  return prefered_res;
}

vector<ResourceID_t>* SJFCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  // No preference arcs in SJF cost model
  return NULL;
}

vector<EquivClass_t>* SJFCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t tec) {
  // There are no internal EC connectors in the SJF cost model
  return NULL;
}

void SJFCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_,
                     ResourceIDFromString(rtnd_ptr->resource_desc().uuid()),
                     rtnd_ptr);
}

void SJFCostModel::AddTask(TaskID_t task_id) {
  // The SJF cost model does not track any task-specific state, so this is
  // a no-op.
}

void SJFCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machine_to_rtnd_.erase(res_id), 1);
}

void SJFCostModel::RemoveTask(TaskID_t task_id) {
  // The SJF cost model does not track any task-specific state, so this is
  // a no-op.
}

FlowGraphNode* SJFCostModel::GatherStats(FlowGraphNode* accumulator,
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

void SJFCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* SJFCostModel::UpdateStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  return accumulator;
}

}  // namespace firmament
