// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Trivial scheduling cost model for testing purposes.

#include "scheduling/flow/trivial_cost_model.h"

#include <SpookyV2.h>
#include <string>
#include <vector>

#include "misc/map-util.h"
#include "misc/utils.h"

DECLARE_bool(preemption);

namespace firmament {

TrivialCostModel::TrivialCostModel(
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids)
  : leaf_res_ids_(leaf_res_ids),
    task_map_(task_map) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
}

Cost_t TrivialCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 5LL;
}

Cost_t TrivialCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t TrivialCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                                ResourceID_t resource_id) {
  return 0LL;
}

Cost_t TrivialCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return 0LL;
}

Cost_t TrivialCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t TrivialCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t TrivialCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                    EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_)
    return 2ULL;
  else
    return 0ULL;
}

pair<Cost_t, int64_t> TrivialCostModel::EquivClassToResourceNode(
    EquivClass_t tec,
    ResourceID_t res_id) {
  return pair<Cost_t, int64_t>(0LL, -1LL);
}

Cost_t TrivialCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                                EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* TrivialCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  string td_binary = td_ptr->binary();
  uint64_t hash = SpookyHash::Hash64(td_binary.c_str(),
                                     sizeof(char) * td_binary.length(), SEED);
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  // All tasks also have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<EquivClass_t>* TrivialCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // Only the cluster aggregator for the trivial cost model
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* TrivialCostModel::GetOutgoingEquivClassPrefArcs(
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

vector<ResourceID_t>* TrivialCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
  for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
       ++num_arc) {
    prefered_res->push_back(PickRandomResourceID(*leaf_res_ids_));
  }
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    TrivialCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  // The trivial cost model does not have any interconnected ECs.
  return pair<vector<EquivClass_t>*, vector<EquivClass_t>*>(NULL, NULL);
}

void TrivialCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_,
                     ResourceIDFromString(rtnd_ptr->resource_desc().uuid()),
                     rtnd_ptr);
}

void TrivialCostModel::AddTask(TaskID_t task_id) {
}

void TrivialCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machine_to_rtnd_.erase(res_id), 1);
}

void TrivialCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* TrivialCostModel::GatherStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* TrivialCostModel::UpdateStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
