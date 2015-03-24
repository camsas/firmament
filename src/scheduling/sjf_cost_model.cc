// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple shortest-job-first scheduling cost model.

#include "scheduling/sjf_cost_model.h"

#include <algorithm>
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

SJFCostModel::SJFCostModel(shared_ptr<TaskMap_t> task_map,
                           unordered_set<ResourceID_t,
                             boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                           KnowledgeBase* kb)
  : task_map_(task_map),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(kb) {
}

const TaskDescriptor& SJFCostModel::GetTask(TaskID_t task_id) {
  TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td);
  return *td;
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t SJFCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t now = GetCurrentTimestamp();
  uint64_t time_since_submit = now - td.submit_time();
  // timestamps are in microseconds, but we scale to tenths of a second here in
  // order to keep the costs small
  uint64_t wait_time_centamillis = time_since_submit / 100000;
  // Cost is the max of the average runtime and the wait time, so that the
  // average runtime is a lower bound on the cost.
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return max(WAIT_TIME_MULTIPLIER * wait_time_centamillis, avg_runtime * 100);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t SJFCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t SJFCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t SJFCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  return TaskToClusterAggCost(task_id);
}

Cost_t SJFCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0LL;
}

// The cost from the resource leaf to the sink is 0.
Cost_t SJFCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t SJFCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t SJFCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t SJFCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                EquivClass_t tec) {
  return 0LL;
}

Cost_t SJFCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                              ResourceID_t res_id) {
  return 0LL;
}

Cost_t SJFCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                            EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* SJFCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* SJFCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  LOG(FATAL) << "Not implemented";
  return NULL;
}

vector<ResourceID_t>* SJFCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
  uint32_t rand_seed_ = 0;
  for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
       ++num_arc) {
    size_t index = rand_r(&rand_seed_) % leaf_res_ids_->size();
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>::iterator it =
      leaf_res_ids_->begin();
    advance(it, index);
    prefered_res->push_back(*it);
  }
  return prefered_res;
}

vector<TaskID_t>* SJFCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* SJFCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    SJFCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(equiv_classes, equiv_classes);
}

void SJFCostModel::AddMachine(const ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void SJFCostModel::RemoveMachine(ResourceID_t res_id) {
}

}  // namespace firmament
