// The Firmament project
// Copyright (c) 2014 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/random_cost_model.h"

#include <set>
#include <string>

#include "misc/map-util.h"
#include "scheduling/common.h"

namespace firmament {

RandomCostModel::RandomCostModel(
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids)
  : task_map_(task_map),
    leaf_res_ids_(leaf_res_ids) {
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t RandomCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand_r(&rand_seed_) % half_max_arc_cost + 1;
}

// The costfrom the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t RandomCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t RandomCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t RandomCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t RandomCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
}

// The cost from the resource leaf to the sink is 0.
Cost_t RandomCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t RandomCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t RandomCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t RandomCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t tec) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t RandomCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                                 ResourceID_t res_id) {
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
}

Cost_t RandomCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                               EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* RandomCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* RandomCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  LOG(FATAL) << "Not implemented";
  return NULL;
}

vector<ResourceID_t>* RandomCostModel::GetOutgoingEquivClassPrefArcs(
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

vector<TaskID_t>* RandomCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* RandomCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    RandomCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(equiv_classes, equiv_classes);
}

void RandomCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void RandomCostModel::AddTask(TaskID_t task_id) {
}

void RandomCostModel::RemoveMachine(ResourceID_t res_id) {
}

void RandomCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* RandomCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* RandomCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
