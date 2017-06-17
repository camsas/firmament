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

#include "scheduling/flow/octopus_cost_model.h"

#include <utility>
#include <vector>

#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"

#define BUSY_PU_OFFSET 100

DECLARE_bool(preemption);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

OctopusCostModel::OctopusCostModel(shared_ptr<ResourceMap_t> resource_map,
                                   shared_ptr<TaskMap_t> task_map)
  : resource_map_(resource_map),
    task_map_(task_map) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
}

ArcDescriptor OctopusCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  return ArcDescriptor(1000000LL, 1ULL, 0ULL);
}

ArcDescriptor OctopusCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

Cost_t OctopusCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 0LL;
}

ArcDescriptor OctopusCostModel::TaskToResourceNode(TaskID_t task_id,
                                                   ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor OctopusCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& src,
    const ResourceDescriptor& dst) {
  if (dst.type() ==  ResourceDescriptor::RESOURCE_PU) {
    string label = dst.friendly_name();
    uint64_t idx = label.find("PU #");
    if (idx != string::npos) {
      string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
      int64_t core_id = strtoll(core_id_substr.c_str(), 0, 10);
      return ArcDescriptor(core_id + dst.num_running_tasks_below() *
                           BUSY_PU_OFFSET, CapacityFromResNodeToParent(dst),
                           0ULL);
    }
  }
  return ArcDescriptor(dst.num_running_tasks_below() * BUSY_PU_OFFSET,
                       CapacityFromResNodeToParent(dst), 0ULL);
}

ArcDescriptor OctopusCostModel::LeafResourceNodeToSink(
    ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor OctopusCostModel::TaskContinuation(TaskID_t task_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor OctopusCostModel::TaskPreemption(TaskID_t task_id) {
  return ArcDescriptor(1000000LL, 1ULL, 0ULL);
}

ArcDescriptor OctopusCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                           EquivClass_t ec) {
  return ArcDescriptor(1LL, 1ULL, 0ULL);
}

ArcDescriptor OctopusCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  Cost_t cost =
    rs->descriptor().num_running_tasks_below() * BUSY_PU_OFFSET;
  return ArcDescriptor(cost, num_free_slots, 0ULL);
}

ArcDescriptor OctopusCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  return ArcDescriptor(0LL, 0ULL, 0ULL);
}

vector<EquivClass_t>* OctopusCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // All tasks have an arc to the cluster aggregator, i.e. they are
  // all in the cluster aggregator EC.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* OctopusCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* arc_destinations = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator, and has arcs to all machines.
    // XXX(malte): This is inefficient, as it needlessly adds all the
    // machines every time we call this. To optimize, we can just include
    // the ones for which arcs are missing.
    for (auto it = machines_.begin();
         it != machines_.end();
         ++it) {
      arc_destinations->push_back(*it);
    }
  }
  return arc_destinations;
}

vector<ResourceID_t>* OctopusCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  // Not used in Octopus cost model
  return NULL;
}

vector<EquivClass_t>* OctopusCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  return NULL;
}

void OctopusCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  // Keep track of the new machine
  CHECK(rtnd_ptr->resource_desc().type() ==
      ResourceDescriptor::RESOURCE_MACHINE);
  machines_.insert(ResourceIDFromString(rtnd_ptr->resource_desc().uuid()));
}

void OctopusCostModel::AddTask(TaskID_t task_id) {
}

void OctopusCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machines_.erase(res_id), 1);
}

void OctopusCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* OctopusCostModel::GatherStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    if (accumulator->type_ == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      if (!accumulator->rd_ptr_)
        return accumulator;
      CHECK_EQ(other->type_, FlowNodeType::SINK);
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

void OctopusCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* OctopusCostModel::UpdateStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  return accumulator;
}

}  // namespace firmament
