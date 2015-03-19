// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Co-ordinated co-location cost model.

#include "scheduling/coco_cost_model.h"

#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

CocoCostModel::CocoCostModel(shared_ptr<TaskMap_t> task_table,
                           KnowledgeBase* kb)
  : knowledge_base_(kb),
    task_table_(task_table) {
}

const TaskDescriptor& CocoCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_table_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t CocoCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
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
Cost_t CocoCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t CocoCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  TaskEquivClass_t ec = GenerateTaskEquivClass(td);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime = knowledge_base_->GetAvgRuntimeForTEC(ec);
  return (avg_runtime * 100);
}

Cost_t CocoCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  return TaskToClusterAggCost(task_id);
}

Cost_t CocoCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0LL;
}

Cost_t CocoCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0LL;
}

// The cost from the resource leaf to the sink is 0.
Cost_t CocoCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t CocoCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t CocoCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t CocoCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return 0LL;
}

Cost_t CocoCostModel::EquivClassToResourceNode(TaskID_t task_id,
                                              ResourceID_t res_id) {
  return 0LL;
}

TaskEquivClass_t CocoCostModel::GetTaskEquivClass(JobID_t job_id) {
  return 0LL;
}

}  // namespace firmament
