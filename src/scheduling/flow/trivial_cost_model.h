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

// Trivial scheduling cost model for testing purposes.

#ifndef FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

class TrivialCostModel : public CostModelInterface {
 public:
  TrivialCostModel(shared_ptr<ResourceMap_t> resource_map,
                   shared_ptr<TaskMap_t> task_map,
                   unordered_set<ResourceID_t,
                     boost::hash<boost::uuids::uuid>>* leaf_res_ids);

  // Costs pertaining to leaving tasks unscheduled
  ArcCostCap TaskToUnscheduledAgg(TaskID_t task_id);
  ArcCostCap UnscheduledAggToSink(JobID_t job_id);
  // Per-task costs (into the resource topology)
  ArcCostCap TaskToResourceNode(TaskID_t task_id, ResourceID_t resource_id);
  // Costs within the resource topology
  ArcCostCap ResourceNodeToResourceNode(
      const ResourceDescriptor& source,
      const ResourceDescriptor& destination);
  ArcCostCap LeafResourceNodeToSink(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  ArcCostCap TaskContinuation(TaskID_t task_id);
  ArcCostCap TaskPreemption(TaskID_t task_id);
  // Costs to equivalence class aggregators
  ArcCostCap TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  ArcCostCap EquivClassToResourceNode(EquivClass_t tec, ResourceID_t res_id);
  ArcCostCap EquivClassToEquivClass(EquivClass_t tec1, EquivClass_t tec2);
  // Get the type of equiv class.
  vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(EquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  vector<EquivClass_t>* GetEquivClassToEquivClassesArcs(EquivClass_t tec);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddTask(TaskID_t task_id);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  void PrepareStats(FlowGraphNode* accumulator);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  shared_ptr<ResourceMap_t> resource_map_;
  // EC corresponding to the CLUSTER_AGG node
  EquivClass_t cluster_aggregator_ec_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  // Mapping betweeen machine res id and resource topology node descriptor.
  unordered_map<ResourceID_t, const ResourceTopologyNodeDescriptor*,
    boost::hash<boost::uuids::uuid>> machine_to_rtnd_;
  // Shared access to the overall set of tasks
  shared_ptr<TaskMap_t> task_map_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H
