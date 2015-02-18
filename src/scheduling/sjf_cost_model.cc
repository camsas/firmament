// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple shortest-job-first scheduling cost model.

#include <string>

#include "scheduling/sjf_cost_model.h"

#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

SJFCostModel::SJFCostModel(shared_ptr<TaskMap_t> task_table,
                           KnowledgeBase* kb)
  : knowledge_base_(kb),
    task_table_(task_table) {
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t SJFCostModel::TaskToUnscheduledAggCost(const TaskDescriptor& td) {
  uint64_t now = GetCurrentTimestamp();
  uint64_t time_since_submit = now - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  return (time_since_submit / 100000);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t SJFCostModel::UnscheduledAggToSinkCost(const JobDescriptor& jd) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t SJFCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t SJFCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_table_, task_id);
  TaskEquivClass_t ec = GenerateTaskEquivClass(*td);
  uint64_t avg_runtime = knowledge_base_->GetAvgRuntimeForTEC(ec);
  return avg_runtime;
}

Cost_t SJFCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

Cost_t SJFCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t SJFCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t SJFCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

/*Cost_t SJFCostModel::TaskToResourceNodeCosts(TaskID_t task_id, const vector<ResourceID_t> &machine_ids,  vector<Cost_t> &machine_task_costs) {
  for (uint64_t i = 0; i < machine_ids.size(); ++i) {
      string host = (*resource_to_host_)[machine_ids[i]];

     if (!knowledge_base_->NumRunningWebservers(host)) {
        machine_task_costs.push_back(0);
      } else {
        machine_task_costs.push_back(2);
      }
  }
  return 1ULL;
}*/

Cost_t SJFCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t SJFCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

}  // namespace firmament
