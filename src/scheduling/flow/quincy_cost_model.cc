// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include "scheduling/flow/quincy_cost_model.h"

#include <set>
#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

QuincyCostModel::QuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_map<TaskID_t, ResourceID_t> *task_bindings,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    shared_ptr<KnowledgeBase> knowledge_base)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    task_bindings_(task_bindings),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(knowledge_base) {
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
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t QuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t QuincyCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 4) + 1;
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

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t tec) {
  return rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 2) + 1;
}

pair<Cost_t, uint64_t> QuincyCostModel::EquivClassToResourceNode(
    EquivClass_t tec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  Cost_t cost = rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 2) + 1;
  return pair<Cost_t, uint64_t>(cost , num_free_slots);
}

Cost_t QuincyCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                               EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* QuincyCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  equiv_classes->push_back(
      static_cast<EquivClass_t>(HashString(td_ptr->binary())));
  return equiv_classes;
}

vector<ResourceID_t>* QuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
  for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
       ++num_arc) {
    prefered_res->push_back(PickRandomResourceID(*leaf_res_ids_));
  }
  return prefered_res;
}

vector<ResourceID_t>* QuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

vector<EquivClass_t>* QuincyCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t tec) {
  return NULL;
}

void QuincyCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void QuincyCostModel::AddTask(TaskID_t task_id) {
}

void QuincyCostModel::RemoveMachine(ResourceID_t res_id) {
}

void QuincyCostModel::RemoveTask(TaskID_t task_id) {
}

void QuincyCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* QuincyCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    // The other node is not a resource node.
    if (other->type_ == FlowNodeType::SINK) {
      // TODO(ionel): This code assumes that only one task can run on a PU.
      if (accumulator->rd_ptr_->has_current_running_task()) {
        accumulator->rd_ptr_->set_num_running_tasks_below(1);
      } else {
        accumulator->rd_ptr_->set_num_running_tasks_below(0);
      }
      accumulator->rd_ptr_->set_num_slots_below(1);
    }
    return accumulator;
  }

  CHECK_NOTNULL(other->rd_ptr_);
  accumulator->rd_ptr_->set_num_running_tasks_below(
      accumulator->rd_ptr_->num_running_tasks_below() +
      other->rd_ptr_->num_running_tasks_below());
  accumulator->rd_ptr_->set_num_slots_below(
      accumulator->rd_ptr_->num_slots_below() +
      other->rd_ptr_->num_slots_below());
  return accumulator;
}

FlowGraphNode* QuincyCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return accumulator;
}

}  // namespace firmament
