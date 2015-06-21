// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

class WhareMapCostModel : public CostModelInterface {
 public:
  WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                    shared_ptr<TaskMap_t> task_map,
                    KnowledgeBase* kb);
  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
  Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                ResourceID_t resource_id);
  // Costs within the resource topology
  Cost_t ResourceNodeToResourceNodeCost(ResourceID_t source,
                                        ResourceID_t destination);
  Cost_t LeafResourceNodeToSinkCost(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  Cost_t TaskContinuationCost(TaskID_t task_id);
  Cost_t TaskPreemptionCost(TaskID_t task_id);
  // Costs to equivalence class aggregators
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  Cost_t EquivClassToResourceNode(EquivClass_t tec, ResourceID_t res_id);
  Cost_t EquivClassToEquivClass(EquivClass_t tec1, EquivClass_t tec2);
  // Get the type of equiv class.
  vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<EquivClass_t>* GetResourceEquivClasses(ResourceID_t res_id);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(EquivClass_t tec);
  vector<TaskID_t>* GetIncomingEquivClassPrefArcs(EquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    GetEquivClassToEquivClassesArcs(EquivClass_t tec);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddTask(TaskID_t task_id);
  void RecordECtoPsPIMapping(pair<EquivClass_t, EquivClass_t> ec_pair,
                             const TaskFinalReport& task_report);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  void AccumulateWhareMapStats(WhareMapStats* accumulator,
                               WhareMapStats* other);
  const TaskDescriptor& GetTask(TaskID_t task_id);
  void ComputeMachineTypeHash(const ResourceTopologyNodeDescriptor* rtnd_ptr,
                              size_t* hash);
  ResourceID_t MachineResIDForResource(ResourceID_t res_id);
  // Cost to cluster aggregator EC
  Cost_t TaskToClusterAggCost(TaskID_t task_id);

  const Cost_t WAIT_TIME_MULTIPLIER = 1;

  // Map of resources present in the system, initialised externally
  shared_ptr<ResourceMap_t> resource_map_;
  // Map of tasks present in the system, initialised externally
  shared_ptr<TaskMap_t> task_map_;
  // A knowledge base instance that we refer to for job runtime statistics,
  // initialised externally
  KnowledgeBase* knowledge_base_;
  // EC corresponding to the CLUSTER_AGG node
  EquivClass_t cluster_aggregator_ec_;
  // Mapping between machine equiv classes and machines.
  multimap<EquivClass_t, ResourceID_t> machine_ec_to_res_id_;
  // Mapping betweeen machine res id and resource topology node descriptor.
  unordered_map<ResourceID_t, const ResourceTopologyNodeDescriptor*,
    boost::hash<boost::uuids::uuid>> machine_to_rtnd_;
  // Mapping between machine res id and task equiv class.
  unordered_map<ResourceID_t, EquivClass_t,
    boost::hash<boost::uuids::uuid>> machine_to_ec_;
  // Mapping between task equiv classes and connected tasks.
  unordered_map<EquivClass_t, set<TaskID_t> > task_ec_to_set_task_id_;
  // Set of task ECs
  unordered_set<EquivClass_t> task_aggs_;
  // Set of machine ECs
  unordered_set<EquivClass_t> machine_aggs_;
  // Map to track <task EC, machine EC> -> PsPI
  // (Psi in the cost model description)
  unordered_map<pair<EquivClass_t, EquivClass_t>, vector<uint64_t>*,
    boost::hash<pair<EquivClass_t, EquivClass_t>>> psi_map_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
