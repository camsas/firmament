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
DEFINE_uint64(machines_per_rack, 30, "Number of machines per rack");
DEFINE_double(quincy_wait_time_factor, 0.5, "The Quincy wait time factor");
DEFINE_double(quincy_preferred_machine_data_fraction, 0.1,
              "Threshold of proportion of data stored on machine for it to be "
              "on preferred list.");
DEFINE_double(quincy_preferred_rack_data_fraction, 0.1,
              "Threshold of proportion of data stored on rack for it to be on "
              "preferred list.");
DEFINE_int64(quincy_tor_transfer_cost, 1,
             "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_int64(quincy_core_transfer_cost, 2,
             "Cost per unit of data transferred in core switch.");
DEFINE_bool(quincy_update_costs_upon_machine_change, true,
            "True if the costs should be updated if a machine is added or "
            "removed");

DECLARE_uint64(max_tasks_per_pu);
DECLARE_bool(generate_quincy_cost_model_trace);

namespace firmament {

QuincyCostModel::QuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    TraceGenerator* trace_generator,
    TimeInterface* time_manager)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    knowledge_base_(knowledge_base),
    trace_generator_(trace_generator),
    time_manager_(time_manager) {
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  data_layer_manager_ = knowledge_base_->mutable_data_layer_manager();
}

QuincyCostModel::~QuincyCostModel() {
  // trace_generator_ and data_layer_manager_ are not owned by QuincyCostModel.
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t QuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  int64_t total_unscheduled_time =
    static_cast<int64_t>(td.total_unscheduled_time()) +
    static_cast<int64_t>(time_manager_->GetCurrentTimestamp()) -
    static_cast<int64_t>(td.submit_time());
  return static_cast<int64_t>(total_unscheduled_time *
                              FLAGS_quincy_wait_time_factor /
                              static_cast<int64_t>(MICROSECONDS_IN_SECOND));
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
  auto machines_data = FindOrNull(task_preferred_machines_, task_id);
  if (machines_data) {
    int64_t* transfer_cost = FindOrNull(*machines_data, resource_id);
    if (transfer_cost) {
      return *transfer_cost;
    } else {
      // The machine is not a preferred one.
      return GetTransferCostToNotPreferredRes(task_id, resource_id);
    }
  } else {
    // The task doesn't have any preferred machines.
    return GetTransferCostToNotPreferredRes(task_id, resource_id);
  }
}

int64_t QuincyCostModel::GetTransferCostToNotPreferredRes(
    TaskID_t task_id,
    ResourceID_t res_id) {
  pair<ResourceID_t, int64_t>* machine_transfer_cost =
    FindOrNull(task_running_arcs_, task_id);
  if (!machine_transfer_cost || machine_transfer_cost->first != res_id) {
    // The running arc did not exist previously or was pointing to a
    // different resource.
    const TaskDescriptor& td = GetTask(task_id);
    ResourceID_t machine_res_id =
      MachineResIDForResource(resource_map_, res_id);
    uint64_t data_on_rack = 0;
    uint64_t data_on_machine = 0;
    uint64_t input_size =
      ComputeDataStatsForMachine(td, machine_res_id, &data_on_rack,
                                 &data_on_machine);
    int64_t transfer_cost =
      ComputeTransferCostToMachine(input_size - data_on_machine,
                                   data_on_rack - data_on_machine);
    // Cache the transfer cost.
    InsertOrUpdate(&task_running_arcs_, task_id,
                   pair<ResourceID_t, int64_t>(res_id, transfer_cost));
    return transfer_cost;
  } else {
    return machine_transfer_cost->second;
  }
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
  const TaskDescriptor& td = GetTask(task_id);
  ResourceID_t pu_res_id = ResourceIDFromString(td.scheduled_to_resource());
  ResourceID_t machine_res_id =
    MachineResIDForResource(resource_map_, pu_res_id);
  Cost_t cost_to_resource = TaskToResourceNodeCost(task_id, machine_res_id);
  // cost_to_resource corresponds to d* and total_running_time corresponds
  // to p* in the Quincy paper.
  return cost_to_resource - static_cast<int64_t>(td.total_run_time());
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return TaskToUnscheduledAggCost(task_id);
}

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t ec) {
  auto ec_costs = FindOrNull(task_preferred_ecs_, task_id);
  CHECK_NOTNULL(ec_costs);
  int64_t* transfer_cost = FindOrNull(*ec_costs, ec);
  CHECK_NOTNULL(transfer_cost);
  return *transfer_cost;
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
    const auto& machines_res_id = data_layer_manager_->GetMachinesInRack(ec2);
    uint64_t capacity = 0;
    for (const auto& machine_res_id : machines_res_id) {
      capacity += GetNumSchedulableSlots(machine_res_id);
    }
    return pair<Cost_t, uint64_t>(0LL, capacity);
  } else {
    LOG(FATAL) << "We only have arcs between cluster agg EC and rack ECs";
  }
}

vector<EquivClass_t>* QuincyCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  auto ecs_data = FindOrNull(task_preferred_ecs_, task_id);
  CHECK_NOTNULL(ecs_data);
  vector<EquivClass_t>* task_ecs = new vector<EquivClass_t>();
  for (auto& ec_data : *ecs_data) {
    task_ecs->push_back(ec_data.first);
  }
  return task_ecs;
}

vector<ResourceID_t>* QuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_) {
    // The cluster aggregator is not directly connected to any resource.
    return NULL;
  } else {
    const auto& rack_machine_res = data_layer_manager_->GetMachinesInRack(ec);
    vector<ResourceID_t>* pref_res =
      new vector<ResourceID_t>(rack_machine_res.begin(),
                               rack_machine_res.end());
    return pref_res;
  }
}

vector<ResourceID_t>* QuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  auto machines_data = FindOrNull(task_preferred_machines_, task_id);
  CHECK_NOTNULL(machines_data);
  vector<ResourceID_t>* preferred_machines = new vector<ResourceID_t>();
  for (auto& machine_data : *machines_data) {
    preferred_machines->push_back(machine_data.first);
  }
  return preferred_machines;
}

vector<EquivClass_t>* QuincyCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  if (ec == cluster_aggregator_ec_) {
    vector<EquivClass_t>* outgoing_ec = new vector<EquivClass_t>();
    data_layer_manager_->GetRackIDs(outgoing_ec);
    return outgoing_ec;
  }
  // The rack aggregators are only connected to resources.
  return NULL;
}

void QuincyCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
  // N.B.: this assumes that the friendly_name field of the RD contains the
  // hostname for machine-type RDs.
  EquivClass_t rack_ec = data_layer_manager_->AddMachine(
     rtnd_ptr->resource_desc().friendly_name(), res_id);
  if (FLAGS_quincy_update_costs_upon_machine_change) {
    for (auto& id_td : *task_map_) {
      // NOTE: task_map_ may contain tasks that have already been removed from
      // the cost model. We only call UpdateTaskCosts for tasks that haven't
      // been removed. We can check if a task has been removed by checking it
      // still exists in the task_preferred_ecs_.
      auto preferred_ecs = FindOrNull(task_preferred_ecs_, id_td.second->uid());
      if (preferred_ecs) {
        UpdateTaskCosts(*(id_td.second), rack_ec, false);
      }
    }
  }
}

void QuincyCostModel::AddTask(TaskID_t task_id) {
  CHECK(InsertIfNotPresent(
      &task_preferred_ecs_, task_id, unordered_map<EquivClass_t, int64_t>()));
  CHECK(InsertIfNotPresent(
      &task_preferred_machines_, task_id,
      unordered_map<ResourceID_t, int64_t, boost::hash<ResourceID_t>>()));
  ConstructTaskPreferredSet(task_id);
}

void QuincyCostModel::RemoveMachine(ResourceID_t res_id) {
  EquivClass_t rack_ec = data_layer_manager_->GetRackForMachine(res_id);
  RemovePreferencesToMachine(res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  bool rack_removed = data_layer_manager_->RemoveMachine(
      rs->topology_node().resource_desc().friendly_name());
  if (rack_removed) {
    RemovePreferencesToRack(rack_ec);
  }
  if (FLAGS_quincy_update_costs_upon_machine_change) {
    for (auto& id_td : *task_map_) {
      // NOTE: task_map_ may contain tasks that have already been removed from
      // the cost model. We only call UpdateTaskCosts for tasks that haven't
      // been removed. We can check if a task has been removed by checking it
      // still exists in the task_preferred_ecs_.
      auto preferred_ecs = FindOrNull(task_preferred_ecs_, id_td.second->uid());
      if (preferred_ecs) {
        UpdateTaskCosts(*(id_td.second), rack_ec, rack_removed);
      }
    }
  }
}

void QuincyCostModel::RemovePreferencesToMachine(ResourceID_t res_id) {
  for (auto& task_to_machines : task_preferred_machines_) {
    ResourceID_t res_id_tmp = res_id;
    task_to_machines.second.erase(res_id_tmp);
  }
}

void QuincyCostModel::RemovePreferencesToRack(EquivClass_t ec) {
  for (auto& task_to_racks : task_preferred_ecs_) {
    task_to_racks.second.erase(ec);
  }
}

void QuincyCostModel::RemoveTask(TaskID_t task_id) {
  task_running_arcs_.erase(task_id);
  task_preferred_ecs_.erase(task_id);
  task_preferred_machines_.erase(task_id);
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

FlowGraphNode* QuincyCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return accumulator;
}

uint64_t QuincyCostModel::ComputeClusterDataStatistics(
    const TaskDescriptor& td,
    unordered_map<ResourceID_t, uint64_t,
      boost::hash<ResourceID_t>>* data_on_machines,
    unordered_map<EquivClass_t, uint64_t>* data_on_racks) {
  CHECK_NOTNULL(data_on_machines);
  CHECK_NOTNULL(data_on_racks);
  unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
                boost::hash<ResourceID_t>> blocks_on_machines;
  unordered_map<EquivClass_t,
                unordered_map<uint64_t, uint64_t>> blocks_on_racks;
  uint64_t input_size = 0;
  for (auto& dependency : td.dependencies()) {
    input_size += dependency.size();
    list<DataLocation> locations;
    data_layer_manager_->GetFileLocations(dependency.location(), &locations);
    for (auto& location : locations) {
      UpdateMachineBlocks(location, &blocks_on_machines);
      UpdateRackBlocks(location, &blocks_on_racks);
    }
  }
  for (auto& machine_blocks : blocks_on_machines) {
    uint64_t data_on_machine = 0;
    for (auto& block_size : machine_blocks.second) {
      data_on_machine += block_size.second;
    }
    CHECK_GE(input_size, data_on_machine);
    CHECK(InsertIfNotPresent(data_on_machines, machine_blocks.first,
                             data_on_machine));
  }
  for (auto& rack_blocks : blocks_on_racks) {
    uint64_t data_on_rack = 0;
    for (auto& block_size : rack_blocks.second) {
      data_on_rack += block_size.second;
    }
    CHECK_GE(input_size, data_on_rack);
    CHECK(InsertIfNotPresent(data_on_racks, rack_blocks.first, data_on_rack));
  }
  return input_size;
}

uint64_t QuincyCostModel::ComputeDataStatsForMachine(
    const TaskDescriptor& td, ResourceID_t machine_res_id,
    uint64_t* data_on_rack, uint64_t* data_on_machine) {
  unordered_set<uint64_t> machine_block_ids;
  unordered_set<uint64_t> rack_block_ids;
  EquivClass_t rack_ec = data_layer_manager_->GetRackForMachine(machine_res_id);
  uint64_t input_size = 0;
  for (auto& dependency : td.dependencies()) {
    input_size += dependency.size();
    list<DataLocation> locations;
    data_layer_manager_->GetFileLocations(dependency.location(), &locations);
    for (auto& location : locations) {
      if (machine_res_id == location.machine_res_id_ &&
          machine_block_ids.insert(location.block_id_).second) {
        *data_on_machine = *data_on_machine + location.size_bytes_;
      }
      if (rack_ec == location.rack_id_ &&
          rack_block_ids.insert(location.block_id_).second) {
        *data_on_rack = *data_on_rack + location.size_bytes_;
      }
    }
  }
  return input_size;
}

int64_t QuincyCostModel::ComputeTransferCostToMachine(uint64_t remote_data,
                                                      uint64_t data_on_rack) {
  CHECK_GE(remote_data, data_on_rack);
  return (FLAGS_quincy_tor_transfer_cost * static_cast<int64_t>(data_on_rack) +
          FLAGS_quincy_core_transfer_cost *
          static_cast<int64_t>(remote_data - data_on_rack)) / BYTES_TO_GB;
}

int64_t QuincyCostModel::ComputeTransferCostToRack(
    EquivClass_t ec,
    uint64_t input_size,
    uint64_t data_on_rack,
    const unordered_map<ResourceID_t, uint64_t,
      boost::hash<ResourceID_t>>& data_on_machines) {
  const auto& machines_in_rack = data_layer_manager_->GetMachinesInRack(ec);
  int64_t cost_worst_machine = INT64_MIN;
  for (const auto& machine_res_id : machines_in_rack) {
    const uint64_t* data_on_machine_ptr =
      FindOrNull(data_on_machines, machine_res_id);
    uint64_t data_on_machine = 0;
    if (data_on_machine_ptr) {
      data_on_machine = *data_on_machine_ptr;
    }
    int64_t cost_to_machine =
      ComputeTransferCostToMachine(input_size - data_on_machine,
                                   data_on_rack - data_on_machine);
    cost_worst_machine = max(cost_worst_machine, cost_to_machine);
  }
  CHECK_GE(cost_worst_machine, 0);
  return cost_worst_machine;
}

void QuincyCostModel::ConstructTaskPreferredSet(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  unordered_map<EquivClass_t, uint64_t> data_on_ecs;
  unordered_map<ResourceID_t, uint64_t,
                boost::hash<ResourceID_t>> data_on_machines;
  // Compute the amount of data the task has on every machine and rack.
  uint64_t input_size =
    ComputeClusterDataStatistics(td, &data_on_machines, &data_on_ecs);

  auto preferred_ecs = FindOrNull(task_preferred_ecs_, task_id);
  CHECK_NOTNULL(preferred_ecs);
  auto preferred_machines = FindOrNull(task_preferred_machines_, task_id);
  CHECK_NOTNULL(preferred_machines);
  int64_t best_machine_cost = INT64_MAX;
  for (auto& machine_data : data_on_machines) {
    uint64_t data_on_machine = machine_data.second;
    if (data_on_machine >=
        input_size * FLAGS_quincy_preferred_machine_data_fraction) {
      // Machine has more data than the required threshold => add it to
      // the preferred list.
      EquivClass_t rack_ec =
        data_layer_manager_->GetRackForMachine(machine_data.first);
      const uint64_t* data_on_rack = FindOrNull(data_on_ecs, rack_ec);
      CHECK_NOTNULL(data_on_rack);
      int64_t transfer_cost =
        ComputeTransferCostToMachine(input_size - data_on_machine,
                                     *data_on_rack - data_on_machine);
      CHECK(InsertIfNotPresent(preferred_machines, machine_data.first,
                               transfer_cost));
      best_machine_cost = min(best_machine_cost, transfer_cost);
    }
  }
  int64_t worst_cluster_cost = INT64_MIN;
  int64_t best_rack_cost = INT64_MAX;
  if (data_on_ecs.size() < data_layer_manager_->GetNumRacks()) {
    // There are racks on which we have no data.
    worst_cluster_cost = FLAGS_quincy_core_transfer_cost *
      static_cast<int64_t>(input_size) / BYTES_TO_GB;
    best_rack_cost = min(best_rack_cost, worst_cluster_cost);
  }
  for (auto& rack_data : data_on_ecs) {
    int64_t transfer_cost =
      ComputeTransferCostToRack(rack_data.first, input_size, rack_data.second,
                                data_on_machines);
    worst_cluster_cost = max(worst_cluster_cost, transfer_cost);
    best_rack_cost = min(best_rack_cost, transfer_cost);
    if (rack_data.second >=
        input_size * FLAGS_quincy_preferred_rack_data_fraction) {
      // Rack has more data than the required threshold => add it to
      // the preferred list.
      CHECK(InsertIfNotPresent(preferred_ecs, rack_data.first, transfer_cost));
    }
  }
  // Add transfer cost to the cluster aggregator.
  CHECK(InsertIfNotPresent(preferred_ecs, cluster_aggregator_ec_,
                           worst_cluster_cost));
  if (FLAGS_generate_quincy_cost_model_trace) {
    TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
    CHECK_NOTNULL(td_ptr);
    trace_generator_->AddTaskQuincy(*td_ptr, input_size, worst_cluster_cost,
                                    best_rack_cost, best_machine_cost,
                                    TaskToUnscheduledAggCost(td_ptr->uid()),
                                    preferred_machines->size(),
                                    preferred_ecs->size() - 1);
  }
}

void QuincyCostModel::UpdateMachineBlocks(
    const DataLocation& location,
    unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
      boost::hash<ResourceID_t>>* data_on_machines) {
  CHECK_NOTNULL(data_on_machines);
  auto machine_blocks =
    FindOrNull(*data_on_machines, location.machine_res_id_);
  if (machine_blocks) {
    InsertOrUpdate(machine_blocks, location.block_id_, location.size_bytes_);
  } else {
    unordered_map<uint64_t, uint64_t> new_machine_blocks;
    InsertIfNotPresent(&new_machine_blocks, location.block_id_,
                       location.size_bytes_);
    InsertIfNotPresent(data_on_machines, location.machine_res_id_,
                       new_machine_blocks);
  }
}

void QuincyCostModel::UpdateRackBlocks(
    const DataLocation& location,
    unordered_map<EquivClass_t,
      unordered_map<uint64_t, uint64_t>>* data_on_racks) {
  CHECK_NOTNULL(data_on_racks);
  auto rack_blocks =
    FindOrNull(*data_on_racks, location.rack_id_);
  if (rack_blocks) {
    InsertOrUpdate(rack_blocks, location.block_id_, location.size_bytes_);
  } else {
    unordered_map<uint64_t, uint64_t> new_rack_blocks;
    InsertIfNotPresent(&new_rack_blocks, location.block_id_,
                       location.size_bytes_);
    InsertIfNotPresent(data_on_racks, location.rack_id_, new_rack_blocks);
  }
}

void QuincyCostModel::UpdateTaskCosts(const TaskDescriptor& td,
                                      EquivClass_t ec_changed,
                                      bool rack_removed) {
  auto preferred_ecs = FindOrNull(task_preferred_ecs_, td.uid());
  // Every task should at least have an arc to the cluster aggregator
  // equivalence class.
  CHECK_NOTNULL(preferred_ecs);
  // TODO(ionel): We don't correctly update the cost for the case in which we
  // remove the worst machine. To correctly handle it we would have to revisit
  // all the ECs and figure out the new worst machine. However, this would be
  // too expensive.
  int64_t* prev_worst_cost = FindOrNull(*preferred_ecs, cluster_aggregator_ec_);
  CHECK_NOTNULL(prev_worst_cost);
  int64_t cost_worst_machine = *prev_worst_cost;
  // Get the worst cost to the racks that haven't changed.
  for (auto& ec_to_cost : *preferred_ecs) {
    if (ec_to_cost.first != cluster_aggregator_ec_ &&
        ec_to_cost.first != ec_changed) {
      cost_worst_machine =
        max(cost_worst_machine, ec_to_cost.second);
    }
  }
  if (!rack_removed) {
    cost_worst_machine =
      max(cost_worst_machine, UpdateTaskCostForRack(td, ec_changed));
  } else {
    preferred_ecs->erase(ec_changed);
  }
  // Update cluster aggregator's cost.
  CHECK_GE(cost_worst_machine, 0);
  InsertOrUpdate(preferred_ecs, cluster_aggregator_ec_, cost_worst_machine);
}

int64_t QuincyCostModel::UpdateTaskCostForRack(const TaskDescriptor& td,
                                               EquivClass_t rack_ec) {
  unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
                boost::hash<ResourceID_t>> machines_blocks;
  unordered_map<uint64_t, uint64_t> rack_blocks;
  const auto& machines_in_rack =
    data_layer_manager_->GetMachinesInRack(rack_ec);
  uint64_t input_size = 0;
  for (auto& dependency : td.dependencies()) {
    input_size += dependency.size();
    list<DataLocation> file_locations;
    data_layer_manager_->GetFileLocations(dependency.location(),
                                          &file_locations);
    for (auto& data_location : file_locations) {
      // Only consider the blocks that are on a machine from the rack we're
      // updating.
      if (machines_in_rack.find(data_location.machine_res_id_) !=
          machines_in_rack.end()) {
        InsertOrUpdate(&rack_blocks, data_location.block_id_,
                       data_location.size_bytes_);
        auto machine_blocks =
          FindOrNull(machines_blocks, data_location.machine_res_id_);
        if (machine_blocks) {
          InsertOrUpdate(machine_blocks, data_location.block_id_,
                         data_location.size_bytes_);
        } else {
          unordered_map<uint64_t, uint64_t> new_machine_blocks;
          InsertIfNotPresent(&new_machine_blocks, data_location.block_id_,
                             data_location.size_bytes_);
          InsertIfNotPresent(&machines_blocks, data_location.machine_res_id_,
                             new_machine_blocks);
        }
      }
    }
  }
  // Compute how much task data we have on the rack. We only account each
  // block one even if it has several copies in the rack.
  uint64_t data_on_rack = 0;
  for (auto& block_size : rack_blocks) {
    data_on_rack += block_size.second;
  }
  // Update the cost for each machine in the rack.
  auto task_pref_machines = FindOrNull(task_preferred_machines_, td.uid());
  uint64_t worst_rack_cost = INT64_MIN;
  for (const auto& machine_res_id : machines_in_rack) {
    auto machine_blocks = FindOrNull(machines_blocks, machine_res_id);
    if (machine_blocks) {
      // Compute the amount of data the task has on the machine.
      uint64_t data_on_machine = 0;
      for (auto& machine_block_size : *machine_blocks) {
        data_on_machine += machine_block_size.second;
      }
      uint64_t transfer_cost =
        ComputeTransferCostToMachine(input_size - data_on_machine,
                                     data_on_rack - data_on_machine);
      task_pref_machines =
        UpdateTaskPreferredMachineList(td.uid(), input_size, machine_res_id,
                                       data_on_machine, transfer_cost,
                                       task_pref_machines);
      worst_rack_cost = max(worst_rack_cost, transfer_cost);
    } else {
      // No blocks on the machine.
      worst_rack_cost = ComputeTransferCostToMachine(input_size, data_on_rack);
    }
  }
  UpdateTaskPreferredRacksList(td.uid(), input_size, data_on_rack,
                               worst_rack_cost, rack_ec);
  return worst_rack_cost;
}

unordered_map<ResourceID_t, int64_t, boost::hash<ResourceID_t>>*
  QuincyCostModel::UpdateTaskPreferredMachineList(
    TaskID_t task_id,
    uint64_t input_size,
    ResourceID_t machine_res_id,
    uint64_t data_on_machine,
    int64_t transfer_cost,
    unordered_map<ResourceID_t, int64_t, boost::hash<ResourceID_t>>*
    task_pref_machines) {
  if (task_pref_machines) {
    // The task has preferences.
    int64_t* machine_transfer_cost =
      FindOrNull(*task_pref_machines, machine_res_id);
    if (machine_transfer_cost) {
      // The machine is already a preferred one.
      if (data_on_machine >=
          input_size * FLAGS_quincy_preferred_machine_data_fraction) {
        *machine_transfer_cost = transfer_cost;
      } else {
        // The machine is not preferred anymore.
        task_pref_machines->erase(machine_res_id);
      }
    } else if (data_on_machine >=
               input_size * FLAGS_quincy_preferred_machine_data_fraction) {
      // The machine has more data than the threshold => add it to the preferred
      // set.
      InsertIfNotPresent(task_pref_machines, machine_res_id, transfer_cost);
    }
  } else if (data_on_machine >=
             input_size * FLAGS_quincy_preferred_machine_data_fraction) {
    // The machine has more data than the threshold => create preferred set for
    // the task and add the machine to it.
    unordered_map<ResourceID_t, int64_t, boost::hash<ResourceID_t>>
      new_pref_machines;
    CHECK(InsertIfNotPresent(&task_preferred_machines_, task_id,
                             new_pref_machines));
    task_pref_machines = FindOrNull(task_preferred_machines_, task_id);
    InsertIfNotPresent(task_pref_machines, machine_res_id, transfer_cost);
  }
  return task_pref_machines;
}

void QuincyCostModel::UpdateTaskPreferredRacksList(
    TaskID_t task_id, uint64_t input_size, uint64_t data_on_rack,
    int64_t worst_rack_cost, EquivClass_t rack_ec) {
  auto pref_ecs = FindOrNull(task_preferred_ecs_, task_id);
  if (pref_ecs) {
    int64_t* rack_cost = FindOrNull(*pref_ecs, rack_ec);
    if (rack_cost) {
      if (data_on_rack >=
          input_size * FLAGS_quincy_preferred_rack_data_fraction) {
        // The rack keeps on being preferred. Update its transfer cost.
        *rack_cost = worst_rack_cost;
      } else {
        // The rack is no longer a preferred one.
        pref_ecs->erase(rack_ec);
      }
    } else {
      if (data_on_rack >=
          input_size * FLAGS_quincy_preferred_rack_data_fraction) {
        // The rack must be added to the preferred list.
        InsertIfNotPresent(pref_ecs, rack_ec, worst_rack_cost);
      }
    }
  } else {
    if (data_on_rack >=
        input_size * FLAGS_quincy_preferred_rack_data_fraction) {
      unordered_map<EquivClass_t, int64_t> new_pref_ecs;
      InsertIfNotPresent(&new_pref_ecs, rack_ec, worst_rack_cost);
      InsertIfNotPresent(&task_preferred_ecs_, task_id, new_pref_ecs);
    }
  }
}

}  // namespace firmament
