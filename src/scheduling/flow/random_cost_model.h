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

#ifndef FIRMAMENT_SCHEDULING_RANDOM_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_RANDOM_COST_MODEL_H

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/common.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

class RandomCostModel : public CostModelInterface {
 public:
  RandomCostModel(shared_ptr<ResourceMap_t> resource_map,
                  shared_ptr<TaskMap_t> task_map,
                  unordered_set<ResourceID_t,
                  boost::hash<boost::uuids::uuid>>* leaf_res_ids);

  // Costs pertaining to leaving tasks unscheduled
  ArcDescriptor TaskToUnscheduledAgg(TaskID_t task_id);
  ArcDescriptor UnscheduledAggToSink(JobID_t job_id);
  // Per-task costs (into the resource topology)
  ArcDescriptor TaskToResourceNode(TaskID_t task_id, ResourceID_t resource_id);
  // Costs within the resource topology
  ArcDescriptor ResourceNodeToResourceNode(
      const ResourceDescriptor& source,
      const ResourceDescriptor& destination);
  ArcDescriptor LeafResourceNodeToSink(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  ArcDescriptor TaskContinuation(TaskID_t task_id);
  ArcDescriptor TaskPreemption(TaskID_t task_id);
  // Costs to equivalence class aggregators
  ArcDescriptor TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  ArcDescriptor EquivClassToResourceNode(EquivClass_t tec, ResourceID_t res_id);
  ArcDescriptor EquivClassToEquivClass(EquivClass_t tec1, EquivClass_t tec2);
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
  // Set of node IDs corresponding to machines
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>> machines_;
  // Set of task ECs
  unordered_set<EquivClass_t> task_aggs_;
  // Mapping between task equiv classes and connected tasks.
  unordered_map<EquivClass_t, unordered_set<TaskID_t> > task_ec_to_set_task_id_;
  // The task map used in the rest of the system
  shared_ptr<TaskMap_t> task_map_;
  // Leaf resource's resource IDs
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  // A seed for the RNG
  uint32_t rand_seed_ = 1234;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_RANDOM_COST_MODEL_H
