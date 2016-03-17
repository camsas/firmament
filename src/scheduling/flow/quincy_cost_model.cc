// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include "scheduling/flow/quincy_cost_model.h"

#include <set>
#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

// Racks contain "between 29 and 31 computers" in Quincy test setup
DEFINE_uint64(quincy_machines_per_rack, 30, "Number of machines per rack");
DEFINE_double(quincy_wait_time_factor, 0.5, "The Quincy wait time factor");

namespace firmament {

QuincyCostModel::QuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    shared_ptr<KnowledgeBase> knowledge_base,
    TimeInterface* time_manager)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(knowledge_base),
    time_manager_(time_manager) {
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
}

QuincyCostModel::~QuincyCostModel() {
  // We don't have to delete leaf_res_ids and time_manager because they are
  // not owned by the cost model.
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t QuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  int64_t task_unscheduled_for =
    (time_manager_->GetCurrentTimestamp() - td.submit_time());
  return task_unscheduled_for * FLAGS_quincy_wait_time_factor /
    MICROSECONDS_IN_SECOND;
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t QuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t QuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t QuincyCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  // Cost between resource nodes is always 0.
  return 0LL;
}

Cost_t QuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  // The cost from the resource leaf to the sink is 0.
  return 0LL;
}

Cost_t QuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  // TODO(ionel): Implement!
  return 0LL;
}

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t ec) {
  // TODO(ionel): Implement!
  if (ec == cluster_aggregator_ec_) {
    // The cost from the task to the cluster aggregator.
    vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
    CHECK_GT(equiv_classes->size(), 0);
    // Avg runtime is in milliseconds, so we convert it to tenths of a second
    uint64_t avg_runtime =
      knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
    delete equiv_classes;
    return (avg_runtime * 100);
  } else {
    // The cost from the task to a rack aggregator.
  }
  return 0LL;
}

pair<Cost_t, uint64_t> QuincyCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  CHECK_NE(ec, cluster_aggregator_ec_);
  uint64_t capacity = GetNumSchedulableSlots(res_id);
  // Cost of arcs from rack aggregators are always zero.
  return pair<Cost_t, uint64_t>(0LL, capacity);
}

pair<Cost_t, uint64_t> QuincyCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  if (ec1 == cluster_aggregator_ec_) {
    // The other equivalence class must be a rack aggregator.
    auto machines_res_id = FindOrNull(rack_to_machine_res_, ec2);
    CHECK_NOTNULL(machines_res_id);
    uint64_t capacity = 0;
    for (auto& machine_res_id : *machines_res_id) {
      capacity += GetNumSchedulableSlots(machine_res_id);
    }
    return pair<Cost_t, uint64_t>(0LL, capacity);
  } else {
    LOG(FATAL) << "We only have arcs between cluster agg EC and rack ECs";
  }
}

vector<EquivClass_t>* QuincyCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  equiv_classes->push_back(cluster_aggregator_ec_);
  // TODO(ionel): Get preferences to racks.
  return equiv_classes;
}

vector<ResourceID_t>* QuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_) {
    // The cluster aggregator is not directly connected to any resource.
    return NULL;
  } else {
    auto rack_machine_res = FindOrNull(rack_to_machine_res_, ec);
    CHECK_NOTNULL(rack_machine_res);
    vector<ResourceID_t>* pref_res =
      new vector<ResourceID_t>(rack_machine_res->begin(),
                               rack_machine_res->end());
    return pref_res;
  }
}

vector<ResourceID_t>* QuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  // TODO(ionel): Get preferences to machines.
  return prefered_res;
}

vector<EquivClass_t>* QuincyCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_) {
    vector<EquivClass_t>* outgoing_ec = new vector<EquivClass_t>();
    // Connect the cluster aggregator to every rack aggregator.
    for (auto& rack_machines : rack_to_machine_res_) {
      outgoing_ec->push_back(rack_machines.first);
    }
    return outgoing_ec;
  }
  // The rack aggregators are only connected to resources.
  return NULL;
}

void QuincyCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  EquivClass_t rack_ec;
  if (racks_with_spare_links_.size() > 0) {
    rack_ec = *(racks_with_spare_links_.begin());
  } else {
    rack_ec = rack_to_machine_res_.size();
    CHECK(InsertIfNotPresent(
        &rack_to_machine_res_, rack_ec,
        unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>()));
    racks_with_spare_links_.insert(rack_ec);
  }
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  machines_in_rack->insert(res_id);
  if (machines_in_rack->size() == FLAGS_quincy_machines_per_rack) {
    racks_with_spare_links_.erase(rack_ec);
  }
  CHECK(InsertIfNotPresent(&machine_to_rack_ec_, res_id, rack_ec));
}

void QuincyCostModel::AddTask(TaskID_t task_id) {
  ConstructTaskPreferedSet(task_id);
  // XXX(ionel): We may want to refresh a task's prefered set.
}

void QuincyCostModel::RemoveMachine(ResourceID_t res_id) {
  EquivClass_t* ec_ptr = FindOrNull(machine_to_rack_ec_, res_id);
  CHECK_NOTNULL(ec_ptr);
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, *ec_ptr);
  CHECK_NOTNULL(machines_in_rack);
  ResourceID_t res_id_tmp = res_id;
  machines_in_rack->erase(res_id_tmp);
  racks_with_spare_links_.insert(*ec_ptr);
  machine_to_rack_ec_.erase(res_id);
  // XXX(ionel): We may want to refresh a task's prefered set.
}

void QuincyCostModel::RemoveTask(TaskID_t task_id) {
  DestructTaskPreferedSet(task_id);
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

void QuincyCostModel::ConstructTaskPreferedSet(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  DataLayerManagerInterface* data_layer =
    knowledge_base_->mutable_data_layer_manager();
  for (auto& dependency : td.dependencies()) {
    data_layer->GetFileLocations(dependency.location());
  }
  // TODO(ionel): Implement!
}

void QuincyCostModel::DestructTaskPreferedSet(TaskID_t task_id) {
  // TODO(ionel): Implement!
}

}  // namespace firmament
