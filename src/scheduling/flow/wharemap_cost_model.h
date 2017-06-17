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

// WhareMap scheduling cost model, as described in the ISCA 2013 paper.

#ifndef FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H

#include <map>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/time_interface.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

class WhareMapCostModel : public CostModelInterface {
 public:
  WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                    shared_ptr<TaskMap_t> task_map,
                    shared_ptr<KnowledgeBase> knowledge_base,
                    TimeInterface* time_manager);
  ~WhareMapCostModel();
  // Debug info export
  virtual const string DebugInfo() const;
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
  uint64_t HashWhareMapStats(const WhareMapStats& wms);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddTask(TaskID_t task_id);
  void RecordMECtoPsPIMapping(pair<EquivClass_t, EquivClass_t> ec_pair,
                              const TaskFinalReport& task_report);
  void RecordMECAndCoRunnerSetToPsPIMapping(
      pair<EquivClass_t, EquivClass_t> ec_pair,
      const WhareMapStats& wms, const TaskFinalReport& task_report);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  void PrepareStats(FlowGraphNode* accumulator);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  void AccumulateWhareMapStats(WhareMapStats* accumulator,
                               WhareMapStats* other);
  Cost_t AverageFromVec(const vector<uint64_t>& vec) const;
  const TaskDescriptor& GetTask(TaskID_t task_id);
  void ComputeMachineTypeHash(const ResourceTopologyNodeDescriptor* rtnd_ptr,
                              size_t* hash);
  uint64_t GetECOutgoingCapacity(EquivClass_t ec);
  vector<EquivClass_t>* GetResourceEquivClasses(ResourceID_t res_id);
  Cost_t MaxFromVec(const vector<uint64_t>& vec) const;
  Cost_t MinFromVec(const vector<uint64_t>& vec) const;
  // Cost to cluster aggregator EC
  Cost_t TaskToClusterAggCost(TaskID_t task_id);

  // Static cost definitions
  const Cost_t WAIT_TIME_MULTIPLIER = 1LL;
  // This lower bound is required to compensate for the core-ID-based cost
  // that we used to balance work across machines on a mostly-idle cluster.
  // To avoid a priority inversion where a task remains unscheduled because only
  // high core IDs are available and their cost exceeds the tasks's unscheduled
  // cost, we add a lower bound of 2*MAX_CORE_ID to the unscheduled cost,
  // ensuring that no path to the sink ever has less than MAX_CORE_ID cost.
  // TODO(malte): Assumes we don't have more than 48 cores for the moment ;-)
  const Cost_t COST_LOWER_BOUND = 96LL;
  // Handy constant to help with the unit conversion
  const uint64_t SECONDS_TO_PICOSECONDS = 1000000000000ULL;

  // Map of resources present in the system, initialised externally
  shared_ptr<ResourceMap_t> resource_map_;
  // Map of tasks present in the system, initialised externally
  shared_ptr<TaskMap_t> task_map_;
  // A knowledge base instance that we refer to for job runtime statistics,
  // initialised externally
  shared_ptr<KnowledgeBase> knowledge_base_;
  // EC corresponding to the CLUSTER_AGG node
  EquivClass_t cluster_aggregator_ec_;
  // Mapping between machine equiv classes and machines.
  unordered_multimap<EquivClass_t, ResourceID_t> machine_ec_to_res_id_;
  // Mapping betweeen machine res id and resource topology node descriptor.
  unordered_map<ResourceID_t, const ResourceTopologyNodeDescriptor*,
    boost::hash<boost::uuids::uuid>> machine_to_rtnd_;
  // Mapping between machine res id and task equiv class.
  unordered_map<ResourceID_t, EquivClass_t,
    boost::hash<boost::uuids::uuid>> machine_to_ec_;
  // Mapping between task equiv classes and connected tasks.
  unordered_map<EquivClass_t, unordered_set<TaskID_t> > task_ec_to_set_task_id_;
  // Set of task ECs
  unordered_set<EquivClass_t> task_aggs_;
  // Set of machine ECs
  unordered_set<EquivClass_t> machine_aggs_;
  // Map to track <task EC, machine EC> -> PsPI
  // (Psi in the cost model description)
  unordered_map<pair<EquivClass_t, EquivClass_t>, vector<uint64_t>*,
    boost::hash<pair<EquivClass_t, EquivClass_t>>> psi_map_;
  // Map to track < <task EC, co-runner set>, machine EC> -> PsPI
  // (Xi in the cost model description)
  unordered_map<pair<pair<EquivClass_t, EquivClass_t>, EquivClass_t>,
    vector<uint64_t>*,
    boost::hash<pair<pair<EquivClass_t, EquivClass_t>, EquivClass_t>>> xi_map_;
  // Map to track task EC -> worst-machine EC PsPI;
  // max_{c_m}(Psi(c_t, c_m))) in the cost model description
  unordered_map<EquivClass_t, uint64_t> worst_case_psi_map_;
  // Map to track task EC -> worst-environment EC PsPI;
  // max_{c_m, L_m}(Xi(c_t, L_m, c_m))) in the cost model description
  unordered_map<EquivClass_t, uint64_t> worst_case_xi_map_;
  // Map to track task EC -> best-machine EC PsPI
  unordered_map<EquivClass_t, uint64_t> best_case_psi_map_;
  // Map to track task EC -> best-environment EC PsPI;
  unordered_map<EquivClass_t, uint64_t> best_case_xi_map_;
  TimeInterface* time_manager_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
