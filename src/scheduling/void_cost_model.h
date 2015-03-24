// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
#ifndef FIRMAMENT_SCHEDULING_VOID_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_VOID_COST_MODEL_H

#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "scheduling/common.h"
#include "misc/utils.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

typedef int64_t Cost_t;

class VoidCostModel : public FlowSchedulingCostModelInterface {
 public:
  VoidCostModel();
  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
  Cost_t TaskToClusterAggCost(TaskID_t task_id);
  Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                ResourceID_t resource_id);
  // Costs within the resource topology
  Cost_t ClusterAggToResourceNodeCost(ResourceID_t target);
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
  vector<TaskEquivClass_t>* GetResourceEquivClasses(ResourceID_t res_id);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(TaskEquivClass_t tec);
  vector<TaskID_t>* GetIncomingEquivClassPrefArcs(TaskEquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>
    GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec);
  void AddMachine(const ResourceTopologyNodeDescriptor* rtnd_ptr);
  void RemoveMachine(ResourceID_t res_id);
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_VOID_COST_MODEL_H
