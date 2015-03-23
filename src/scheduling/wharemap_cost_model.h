// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// WhareMap scheduling cost model, as described in the ISCA 2013 paper.

#ifndef FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "misc/utils.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

typedef int64_t Cost_t;

class WhareMapCostModel : public FlowSchedulingCostModelInterface {
 public:
  WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                    shared_ptr<TaskMap_t> task_map,
                    KnowledgeBase* kb);
  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
  Cost_t TaskToClusterAggCost(TaskID_t task_id);
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
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id, TaskEquivClass_t tec);
  Cost_t EquivClassToResourceNode(TaskEquivClass_t tec, ResourceID_t res_id);
  Cost_t EquivClassToEquivClass(TaskEquivClass_t tec1, TaskEquivClass_t tec2);
  // Get the type of equiv class.
  vector<TaskEquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<ResourceID_t>* GetEquivClassPreferenceArcs(TaskEquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>
    GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec);
  void AddMachine(const ResourceTopologyNodeDescriptor* rtnd_ptr);
  void RemoveMachine(ResourceID_t res_id);

 private:
  const TaskDescriptor& GetTask(TaskID_t task_id);
  void ComputeMachineTypeHash(const ResourceTopologyNodeDescriptor* rtnd_ptr,
                              size_t* hash);

  shared_ptr<ResourceMap_t> resource_map_;
  shared_ptr<TaskMap_t> task_map_;
  // Mapping between machine equiv classes and machines.
  multimap<TaskEquivClass_t, ResourceID_t> machine_ec_to_res_id_;
  // Mapping betweeen machine res id and resource topology node descriptor.
  unordered_map<ResourceID_t, const ResourceTopologyNodeDescriptor*,
    boost::hash<boost::uuids::uuid>> machine_to_rtnd_;
  // Mapping between machine res id and task equiv class.
  unordered_map<ResourceID_t, TaskEquivClass_t,
    boost::hash<boost::uuids::uuid>> machine_to_ec_;

  unordered_set<TaskEquivClass_t> task_aggs_;
  unordered_set<TaskEquivClass_t> machine_aggs_;
  // A knowledge base instance that we will refer to for job runtime statistics.
  KnowledgeBase* knowledge_base_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
