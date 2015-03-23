// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// WhareMap cost model.

#include "scheduling/wharemap_cost_model.h"

#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

WhareMapCostModel::WhareMapCostModel(shared_ptr<ResourceMap_t> resource_map,
                                     shared_ptr<TaskMap_t> task_map,
                                     KnowledgeBase* kb)
  : resource_map_(resource_map),
    task_map_(task_map),
    knowledge_base_(kb) {
}

const TaskDescriptor& WhareMapCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t WhareMapCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
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
Cost_t WhareMapCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t WhareMapCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
  vector<TaskEquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t WhareMapCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                                 ResourceID_t resource_id) {
  // TODO(ionel): Implement!
  return TaskToClusterAggCost(task_id);
}

Cost_t WhareMapCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  // TODO(ionel): Implement!
  return 0LL;
}

// The cost from the resource leaf to the sink is 0.
Cost_t WhareMapCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t WhareMapCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t WhareMapCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t WhareMapCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                     TaskEquivClass_t tec) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t WhareMapCostModel::EquivClassToResourceNode(TaskEquivClass_t tec,
                                                   ResourceID_t res_id) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t WhareMapCostModel::EquivClassToEquivClass(TaskEquivClass_t tec1,
                                                 TaskEquivClass_t tec2) {
  // TODO(ionel): Implement!
  return 0LL;
}

vector<TaskEquivClass_t>* WhareMapCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* WhareMapCostModel::GetEquivClassPreferenceArcs(
    TaskEquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* WhareMapCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<TaskEquivClass_t>*, vector<TaskEquivClass_t>*>
    WhareMapCostModel::GetEquivClassToEquivClassesArcs(TaskEquivClass_t tec) {
  vector<TaskEquivClass_t>* equiv_classes = new vector<TaskEquivClass_t>();
  return pair<vector<TaskEquivClass_t>*,
              vector<TaskEquivClass_t>*>(equiv_classes, equiv_classes);
}

void WhareMapCostModel::AddMachine(
    const ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void WhareMapCostModel::RemoveMachine(ResourceID_t res_id) {
}

}  // namespace firmament
