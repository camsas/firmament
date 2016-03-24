// The Firmament project
// Copyright (c) 2014 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/random_cost_model.h"

#include <set>
#include <string>

#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/common.h"

DECLARE_bool(preemption);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

RandomCostModel::RandomCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids)
  : resource_map_(resource_map),
    task_map_(task_map),
    leaf_res_ids_(leaf_res_ids) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
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

Cost_t RandomCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t RandomCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 4) + 1;
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
                                                   EquivClass_t ec) {
  // The cost of scheduling via the cluster aggregator; always slightly
  // less than the cost of leaving the task unscheduled
  if (ec == cluster_aggregator_ec_)
    return rand_r(&rand_seed_) % TaskToUnscheduledAggCost(task_id) - 1;
  else
    // XXX(malte): Implement other EC's costs!
    return 0;
}

pair<Cost_t, uint64_t> RandomCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  Cost_t cost = rand_r(&rand_seed_) % (FLAGS_flow_max_arc_cost / 2) + 1;
  return pair<Cost_t, uint64_t>(cost, num_free_slots);
}

pair<Cost_t, uint64_t> RandomCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  return pair<Cost_t, uint64_t>(0LL, 0ULL);
}

vector<EquivClass_t>* RandomCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // All tasks have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  // An additional TEC is the hash of the task binary name.
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashString(td_ptr->binary()));
  equiv_classes->push_back(task_agg);
  task_aggs_.insert(task_agg);
  unordered_map<EquivClass_t, set<TaskID_t> >::iterator task_ec_it =
    task_ec_to_set_task_id_.find(task_agg);
  if (task_ec_it != task_ec_to_set_task_id_.end()) {
    task_ec_it->second.insert(task_id);
  } else {
    set<TaskID_t> task_set;
    task_set.insert(task_id);
    CHECK(InsertIfNotPresent(&task_ec_to_set_task_id_, task_agg, task_set));
  }
  return equiv_classes;
}

vector<ResourceID_t>* RandomCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* arc_destinations = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // Cluster aggregator, put arcs to all machines
    for (auto it = machines_.begin();
         it != machines_.end();
         ++it) {
      arc_destinations->push_back(*it);
    }
  } else if (task_aggs_.find(ec) != task_aggs_.end()) {
    // Task equivalence class, put some random preference arcs
    CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
    for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
         ++num_arc) {
      arc_destinations->push_back(PickRandomResourceID(*leaf_res_ids_));
    }
  }
  return arc_destinations;
}

vector<ResourceID_t>* RandomCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

vector<EquivClass_t>* RandomCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  // Not used in the random cost model
  return NULL;
}

void RandomCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  // Keep track of the new machine
  CHECK(rtnd_ptr->resource_desc().type() ==
      ResourceDescriptor::RESOURCE_MACHINE);
  machines_.insert(ResourceIDFromString(rtnd_ptr->resource_desc().uuid()));
}

void RandomCostModel::AddTask(TaskID_t task_id) {
}

void RandomCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machines_.erase(res_id), 1);
}

void RandomCostModel::RemoveTask(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  for (vector<EquivClass_t>::iterator it = equiv_classes->begin();
       it != equiv_classes->end(); ++it) {
    unordered_map<EquivClass_t, set<TaskID_t> >::iterator set_it =
      task_ec_to_set_task_id_.find(*it);
    if (set_it != task_ec_to_set_task_id_.end()) {
      set_it->second.erase(task_id);
      if (set_it->second.size() == 0) {
        task_ec_to_set_task_id_.erase(*it);
        task_aggs_.erase(*it);
      }
    }
  }
}

FlowGraphNode* RandomCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (other->resource_id_.is_nil()) {
    // The other node is not a resource node.
    if (other->type_ == FlowNodeType::SINK) {
      accumulator->rd_ptr_->set_num_running_tasks_below(
          accumulator->rd_ptr_->current_running_tasks_size());
      accumulator->rd_ptr_->set_num_slots_below(FLAGS_max_tasks_per_pu);
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

void RandomCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* RandomCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return accumulator;
}

}  // namespace firmament
