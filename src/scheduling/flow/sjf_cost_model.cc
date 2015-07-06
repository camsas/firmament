// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple shortest-job-first scheduling cost model.

#include "scheduling/flow/sjf_cost_model.h"

#include <algorithm>
#include <string>
#include <unordered_map>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

SJFCostModel::SJFCostModel(shared_ptr<TaskMap_t> task_map,
                           unordered_set<ResourceID_t,
                             boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                           KnowledgeBase* kb)
  : knowledge_base_(kb),
    task_map_(task_map) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
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
                                                EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_)
    return TaskToClusterAggCost(task_id) + 1;
  else
    return 0ULL;
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
  // All tasks also have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<EquivClass_t>* SJFCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // Only the cluster aggregator for the trivial cost model
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* SJFCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator, and has arcs to all machines.
    // XXX(malte): This is inefficient, as it needlessly adds all the
    // machines every time we call this. To optimize, we can just include
    // the ones for which arcs are missing.
    for (auto it = machine_to_rtnd_.begin();
         it != machine_to_rtnd_.end();
         ++it) {
      prefered_res->push_back(it->first);
    }
  }
  return prefered_res;
}

vector<TaskID_t>* SJFCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<TaskID_t>* tasks_with_incoming_arcs = new vector<TaskID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator.
    // We add an arc from each task to the cluster aggregator.
    // XXX(malte): This is very slow because it iterates over all tasks; we
    // should instead only return the set of tasks that do not yet have the
    // appropriate arcs.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      // XXX(malte): task_map_ contains ALL tasks ever seen by the system,
      // including those that have completed, failed or are otherwise no longer
      // present in the flow graph. We do some crude filtering here, but clearly
      // we should instead maintain a collection of tasks actually eligible for
      // scheduling.
      if (it->second->state() == TaskDescriptor::RUNNABLE ||
          it->second->state() == TaskDescriptor::RUNNING)
        tasks_with_incoming_arcs->push_back(it->first);
    }
  }
  return tasks_with_incoming_arcs;
}

vector<ResourceID_t>* SJFCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  // No preference arcs in SJF cost model
  return NULL;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    SJFCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  // There are no internal EC connectors in the SJF cost model
  return pair<vector<EquivClass_t>*, vector<EquivClass_t>*>(NULL, NULL);
}

void SJFCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_,
                     ResourceIDFromString(rtnd_ptr->resource_desc().uuid()),
                     rtnd_ptr);
}

void SJFCostModel::AddTask(TaskID_t task_id) {
  // The SJF cost model does not track any task-specific state, so this is
  // a no-op.
}

void SJFCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machine_to_rtnd_.erase(res_id), 1);
}

void SJFCostModel::RemoveTask(TaskID_t task_id) {
  // The SJF cost model does not track any task-specific state, so this is
  // a no-op.
}

FlowGraphNode* SJFCostModel::GatherStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  // No statistics tracking exists in the SJF cost model
  return NULL;
}

FlowGraphNode* SJFCostModel::UpdateStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  // No statistics tracking exists in the SJF cost model
  return NULL;
}

}  // namespace firmament
