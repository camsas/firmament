// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Abstract class representing the interface for cost model implementations.

#ifndef FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H

#include <string>

#include "base/common.h"
#include "base/types.h"

namespace firmament {

typedef uint64_t Cost_t;

class FlowSchedulingCostModelInterface {
 public:
  FlowSchedulingCostModelInterface();

  // Costs pertaining to leaving tasks unscheduled
  virtual Cost_t TaskToUnscheduledAggCost(TaskID_t task_id) = 0;
  virtual Cost_t UnscheduledAggToSinkCost(JobID_t job_id) = 0;
  // Per-task costs (into the resource topology)
  virtual Cost_t TaskToClusterAggCost(TaskID_t task_id) = 0;
  virtual Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                ResourceID_t resource_id) = 0;
  // Costs within the resource topology
  virtual Cost_t ClusterAggToResourceNodeCost(ResourceID_t target) = 0;
  virtual Cost_t ResourceNodeToResourceNodeCost(ResourceID_t source,
                                        ResourceID_t destination) = 0;
  virtual Cost_t LeafResourceNodeToSinkCost(ResourceID_t resource_id) = 0;
  // Costs pertaining to preemption (i.e. already running tasks)
  virtual Cost_t TaskContinuationCost(TaskID_t task_id) = 0;
  virtual Cost_t TaskPreemptionCost(TaskID_t task_id) = 0;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H
