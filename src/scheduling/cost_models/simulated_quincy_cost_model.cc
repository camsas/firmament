// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include "scheduling/cost_models/simulated_quincy_cost_model.h"

#include <set>
#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"
#include "scheduling/knowledge_base.h"

namespace firmament {

SimulatedQuincyCostModel::SimulatedQuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_map<TaskID_t, ResourceID_t> *task_bindings,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    KnowledgeBase* kb)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    task_bindings_(task_bindings),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(kb) {
	// TODO
  //application_stats_ = knowledge_base_->AppStats();
  CHECK_NOTNULL(task_bindings_);
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t SimulatedQuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
	// TODO
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand_r(&rand_seed_) % half_max_arc_cost + 1;
  //  return 5ULL;
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t SimulatedQuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t SimulatedQuincyCostModel::TaskToClusterAggCost(TaskID_t task_id) {
	// TODO
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t SimulatedQuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
	// TODO
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t SimulatedQuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
	// TODO
	// XXX: When is this called?
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t SimulatedQuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

// XXX: Are we going to support task preemption? Otherwise I fear the task is
// a little trivial.
Cost_t SimulatedQuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t SimulatedQuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t SimulatedQuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t tec) {
	// TODO
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t SimulatedQuincyCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                                 ResourceID_t res_id) {
	// TODO
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t SimulatedQuincyCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                               EquivClass_t tec2) {
	// XXX: When is this called?
  return 0LL;
}

vector<EquivClass_t>* SimulatedQuincyCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
	// TODO
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* SimulatedQuincyCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
	// TODO: rack aggregators?
  LOG(FATAL) << "Not implemented";
  return NULL;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
	// TODO
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

vector<TaskID_t>* SimulatedQuincyCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
	// TODO: rack aggregators?
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
	// TODO: have some preferences
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    SimulatedQuincyCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
	// TODO: is it right for this to be empty?
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
             (equiv_classes, equiv_classes);
}

void SimulatedQuincyCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
	// TODO: ? Should generate some datasets for it
}

void SimulatedQuincyCostModel::RemoveMachine(ResourceID_t res_id) {
	// TODO: unclear how best to handle, perhaps shuffle datasets to other machines
	// (in practice, it would be replicated.)
}

void SimulatedQuincyCostModel::RemoveTask(TaskID_t task_id) {
	// TODO: perhaps this can be a no-op for Quincy? It doesn't affect the
	// data distribution at all
}

// TODO: think these should be no-op, confirm
FlowGraphNode* SimulatedQuincyCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* SimulatedQuincyCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
