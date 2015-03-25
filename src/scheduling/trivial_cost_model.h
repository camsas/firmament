// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#ifndef FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

typedef int64_t Cost_t;

class TrivialCostModel : public FlowSchedulingCostModelInterface {
 public:
  TrivialCostModel(shared_ptr<TaskMap_t> task_map,
                   unordered_set<ResourceID_t,
                     boost::hash<boost::uuids::uuid>>* leaf_res_ids);

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
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id,
                                    EquivClass_t tec);
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
  void AddMachine(const ResourceTopologyNodeDescriptor* rtnd_ptr);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);

 private:
  shared_ptr<TaskMap_t> task_map_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_TRIVIAL_COST_MODEL_H
