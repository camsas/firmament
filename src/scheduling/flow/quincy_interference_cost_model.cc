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

#include "scheduling/flow/quincy_interference_cost_model.h"

#include <set>
#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"

DEFINE_double(quincy_interfence_wait_time_factor, 0.5,
              "The Quincy wait time factor");
DEFINE_double(quincy_interfence_preferred_machine_data_fraction, 0.1,
              "Threshold of proportion of data stored on machine for it to be "
              "on preferred list.");
DEFINE_double(quincy_interfence_preferred_rack_data_fraction, 0.1,
              "Threshold of proportion of data stored on rack for it to be on "
              "preferred list.");
DEFINE_int64(quincy_interfence_tor_transfer_cost, 1,
             "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_int64(quincy_interfence_core_transfer_cost, 2,
             "Cost per unit of data transferred in core switch.");
DEFINE_bool(quincy_interfence_update_costs_upon_machine_change, true,
            "True if the costs should be updated if a machine is added or "
            "removed");
DEFINE_int64(quincy_interfence_positive_cost_offset, 2592000, "Value to offset "
             "costs so that they don't go negative. This value should be bigger"
             " than the runtime (in sec) of the longest task");
DEFINE_bool(quincy_interference_no_scheduling_delay, false, "Offset cost to "
            "unscheduled aggregator so that tasks get scheduled as soon as "
            "possible");

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

QuincyInterferenceCostModel::QuincyInterferenceCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    TraceGenerator* trace_generator,
    TimeInterface* time_manager)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    knowledge_base_(knowledge_base),
    trace_generator_(trace_generator),
    time_manager_(time_manager) {
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  data_layer_manager_ = knowledge_base_->mutable_data_layer_manager();
}

QuincyInterferenceCostModel::~QuincyInterferenceCostModel() {
  // trace_generator_ and data_layer_manager_ are not owned by
  // QuincyInterferenceCostModel.
}

ArcDescriptor QuincyInterferenceCostModel::TaskToUnscheduledAgg(
    TaskID_t task_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::UnscheduledAggToSink(
    JobID_t job_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::TaskToResourceNode(
    TaskID_t task_id,
    ResourceID_t resource_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::LeafResourceNodeToSink(
    ResourceID_t resource_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::TaskToEquivClassAggregator(
    TaskID_t task_id,
    EquivClass_t ec) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyInterferenceCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  // TODO(ionel): Implement.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

vector<EquivClass_t>* QuincyInterferenceCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  // TODO(ionel): Implement.
  return NULL;
}

vector<ResourceID_t>*
QuincyInterferenceCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  // TODO(ionel): Implement.
  return NULL;
}

vector<ResourceID_t>* QuincyInterferenceCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  // TODO(ionel): Implement.
  return NULL;
}

vector<EquivClass_t>*
QuincyInterferenceCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  // TODO(ionel): Implement.
  return NULL;
}

void QuincyInterferenceCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  // TODO(ionel): Implement.
}

void QuincyInterferenceCostModel::AddTask(TaskID_t task_id) {
  // TODO(ionel): Implement.
}

void QuincyInterferenceCostModel::RemoveMachine(ResourceID_t res_id) {
  // TODO(ionel): Implement.
}

void QuincyInterferenceCostModel::RemoveTask(TaskID_t task_id) {
  // TODO(ionel): Implement.
}

void QuincyInterferenceCostModel::PrepareStats(FlowGraphNode* accumulator) {
  // TODO(ionel): Implement.
}

FlowGraphNode* QuincyInterferenceCostModel::GatherStats(
    FlowGraphNode* accumulator,
    FlowGraphNode* other) {
  // TODO(ionel): Implement.
  return accumulator;
}

FlowGraphNode* QuincyInterferenceCostModel::UpdateStats(
    FlowGraphNode* accumulator,
    FlowGraphNode* other) {
  // TODO(ionel): Implement.
  return accumulator;
}

}  // namespace firmament
