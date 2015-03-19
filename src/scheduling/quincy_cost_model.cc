// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/quincy_cost_model.h"

namespace firmament {

QuincyCostModel::QuincyCostModel(shared_ptr<ResourceMap_t> resource_map,
                                 shared_ptr<JobMap_t> job_map,
                                 shared_ptr<TaskMap_t> task_map,
                                 map<TaskID_t, ResourceID_t> *task_bindings,
                                 KnowledgeBase* kb)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    task_bindings_(task_bindings),
    knowledge_base_(kb) {
  //application_stats_ = knowledge_base_->AppStats();
  CHECK_NOTNULL(task_bindings_);
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t QuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand_r(&rand_seed_) % half_max_arc_cost + 1;
  //  return 5ULL;
}

// The costfrom the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t QuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t QuincyCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  TaskDescriptor** td_ptr = FindOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  TaskEquivClass_t ec = GenerateTaskEquivClass(**td_ptr);
  uint64_t avg_runtime = knowledge_base_->GetAvgRuntimeForTEC(ec);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  return (avg_runtime * 100);
}

Cost_t QuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t QuincyCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

Cost_t QuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t QuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t QuincyCostModel::EquivClassToResourceNode(TaskID_t task_id,
                                                 ResourceID_t res_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

TaskEquivClass_t QuincyCostModel::GetTaskEquivClass(JobID_t job_id) {
  return 0LL;
}

}  // namespace firmament
