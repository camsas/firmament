// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// WhareMap scheduling cost model, as described in the ISCA 2013 paper.

#ifndef FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H


#include <string>
#include <unordered_map>

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
  WhareMapCostModel(shared_ptr<TaskMap_t> task_table,
               KnowledgeBase* kb);
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
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id);
  Cost_t EquivClassToResourceNode(TaskID_t task_id, ResourceID_t res_id);
  // Get the type of equiv class.
  TaskEquivClass_t GetTaskEquivClass(JobID_t job_id);

 private:
  const TaskDescriptor& GetTask(TaskID_t task_id);

  // A knowledge base instance that we will refer to for job runtime statistics.
  KnowledgeBase* knowledge_base_;
  shared_ptr<TaskMap_t> task_table_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_WHAREMAP_COST_MODEL_H
