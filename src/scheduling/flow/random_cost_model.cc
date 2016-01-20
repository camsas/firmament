// The Firmament project
// Copyright (c) 2014 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/random_cost_model.h"

#include <set>
#include <string>

#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/common.h"

DECLARE_bool(preemption);

namespace firmament {

RandomCostModel::RandomCostModel(
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids)
  : task_map_(task_map),
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

pair<Cost_t, int64_t> RandomCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  return pair<Cost_t, int64_t>(rand_r(&rand_seed_) %
                               (FLAGS_flow_max_arc_cost / 2) + 1, -1LL);
}

Cost_t RandomCostModel::EquivClassToEquivClass(EquivClass_t ec1,
                                               EquivClass_t ec2) {
  return 0LL;
}

vector<EquivClass_t>* RandomCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // All tasks have an arc to the cluster aggregator.
  equiv_classes->push_back(cluster_aggregator_ec_);
  // An additional TEC is the hash of the task binary name.
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  EquivClass_t task_agg = static_cast<EquivClass_t>(hash);
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

vector<EquivClass_t>* RandomCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // Every machine is in the special cluster aggregator EC
  equiv_classes->push_back(cluster_aggregator_ec_);
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

vector<TaskID_t>* RandomCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<TaskID_t>* prefered_task = new vector<TaskID_t>();
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
          (FLAGS_preemption &&
           it->second->state() == TaskDescriptor::RUNNING)) {
        prefered_task->push_back(it->first);
      }
    }
  } else if (task_aggs_.find(ec) != task_aggs_.end()) {
    // ec is a task aggregator.
    // This is where we add preference arcs from tasks to new equiv class
    // aggregators.
    // XXX(ionel): This is very slow because it iterates over all tasks.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      size_t hash = 0;
      boost::hash_combine(hash, it->second->binary());
      EquivClass_t task_ec = static_cast<EquivClass_t>(hash);
      if (task_ec == ec) {
        prefered_task->push_back(it->first);
      }
    }
  } else {
    LOG(FATAL) << "Unexpected type of task equivalence aggregator";
  }
  return prefered_task;
}

vector<ResourceID_t>* RandomCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  // Tasks do not have preference arcs to resources, but all tasks
  // have arcs to the cluster aggregator. This arc is added in
  // GetIncomingEquivClassPrefArcs(), rather than here, since this
  // method returns a vector of ResourceID_t, i.e. it is used for
  // direct preferences to resources.
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    RandomCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t ec) {
  // Not used in the random cost model
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(NULL, NULL);
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
  // No-op in random cost model
  return NULL;
}

FlowGraphNode* RandomCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  // No-op in random cost model
  return NULL;
}

}  // namespace firmament
