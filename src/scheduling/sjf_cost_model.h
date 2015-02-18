// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#ifndef FIRMAMENT_SCHEDULING_SJF_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_SJF_COST_MODEL_H


#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "engine/knowledge_base.h"
#include "scheduling/common.h"
#include "misc/utils.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

typedef int64_t Cost_t;

class SJFCostModel : public FlowSchedulingCostModelInterface {
 public:
  SJFCostModel(shared_ptr<TaskMap_t> task_table,
               KnowledgeBase* kb);
  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(const TaskDescriptor& td);
  Cost_t UnscheduledAggToSinkCost(const JobDescriptor& jd);
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

 private:
  // A knowledge base instance that we will refer to for job runtime statistics.
  KnowledgeBase* knowledge_base_;
  shared_ptr<TaskMap_t> task_table_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_SJF_COST_MODEL_H
