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
DEFINE_double(quincy_prefered_machine_data_fraction, 0.1,
              "Threshold of proportion of data stored on machine for it to be "
              "on preferred list.");
DEFINE_double(quincy_prefered_rack_data_fraction, 0.1,
              "Threshold of proportion of data stored on rack for it to be on "
              "preferred list.");
DEFINE_uint64(quincy_tor_transfer_cost, 1,
              "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_uint64(quincy_core_transfer_cost, 2,
              "Cost per unit of data transferred in core switch.");
DEFINE_bool(quincy_update_costs_upon_machine_change, true,
            "True if the costs should be updated if a machine is added or "
            "removed");

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
  return td.total_unscheduled_time() * FLAGS_quincy_wait_time_factor /
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
  auto machines_data = FindOrNull(task_prefered_machines_, task_id);
  CHECK_NOTNULL(machines_data);
  DataCost* data_on_machine = FindOrNull(*machines_data, resource_id);
  CHECK_NOTNULL(data_on_machine);
  return data_on_machine->transfer_cost_;
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
  return cost_to_resource - td.total_run_time();
}

Cost_t QuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return TaskToUnscheduledAggCost(task_id);
}

Cost_t QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t ec) {
  auto ec_costs = FindOrNull(task_prefered_ecs_, task_id);
  CHECK_NOTNULL(ec_costs);
  DataCost* data_on_ec = FindOrNull(*ec_costs, ec);
  CHECK_NOTNULL(data_on_ec);
  return data_on_ec->transfer_cost_;
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
  auto ecs_data = FindOrNull(task_prefered_ecs_, task_id);
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
    auto rack_machine_res = FindOrNull(rack_to_machine_res_, ec);
    CHECK_NOTNULL(rack_machine_res);
    vector<ResourceID_t>* pref_res =
      new vector<ResourceID_t>(rack_machine_res->begin(),
                               rack_machine_res->end());
    return pref_res;
  }
}

vector<ResourceID_t>* QuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  auto machines_data = FindOrNull(task_prefered_machines_, task_id);
  CHECK_NOTNULL(machines_data);
  vector<ResourceID_t>* prefered_machines = new vector<ResourceID_t>();
  for (auto& machine_data : *machines_data) {
    prefered_machines->push_back(machine_data.first);
  }
  return prefered_machines;
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
    // Assign the machine to a rack that has spare links.
    rack_ec = *(racks_with_spare_links_.begin());
  } else {
    // Add a new rack.
    rack_ec = rack_to_machine_res_.size();
    CHECK(InsertIfNotPresent(
        &rack_to_machine_res_, rack_ec,
        unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>()));
    racks_with_spare_links_.insert(rack_ec);
  }
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  machines_in_rack->insert(res_id);
  // Erase the rack from the spare_links set if the rack is now full.
  if (machines_in_rack->size() == FLAGS_quincy_machines_per_rack) {
    racks_with_spare_links_.erase(rack_ec);
  }
  CHECK(InsertIfNotPresent(&machine_to_rack_ec_, res_id, rack_ec));
  if (FLAGS_quincy_update_costs_upon_machine_change) {
    for (auto& id_td : *task_map_) {
      UpdateTaskDataCosts(*(id_td.second), rack_ec);
    }
  }
}

void QuincyCostModel::AddTask(TaskID_t task_id) {
  CHECK(InsertIfNotPresent(
      &task_prefered_ecs_, task_id, unordered_map<EquivClass_t, DataCost>()));
  CHECK(InsertIfNotPresent(
      &task_prefered_machines_, task_id,
      unordered_map<ResourceID_t,
        DataCost, boost::hash<boost::uuids::uuid>>()));
  ConstructTaskPreferedSet(task_id);
}

void QuincyCostModel::RemoveMachine(ResourceID_t res_id) {
  EquivClass_t* ec_ptr = FindOrNull(machine_to_rack_ec_, res_id);
  CHECK_NOTNULL(ec_ptr);
  RemovePreferencesToMachine(res_id);
  bool rack_removed = false;
  RemoveMachineFromRack(res_id, *ec_ptr, &rack_removed);
  if (!rack_removed && FLAGS_quincy_update_costs_upon_machine_change) {
    for (auto& id_td : *task_map_) {
      UpdateTaskDataCosts(*(id_td.second), *ec_ptr);
    }
  }
}

void QuincyCostModel::RemovePreferencesToMachine(ResourceID_t res_id) {
  for (auto& task_to_machines : task_prefered_machines_) {
    ResourceID_t res_id_tmp = res_id;
    task_to_machines.second.erase(res_id_tmp);
  }
}

void QuincyCostModel::RemovePreferencesToRack(EquivClass_t ec) {
  for (auto& task_to_racks : task_prefered_ecs_) {
    task_to_racks.second.erase(ec);
  }
}

void QuincyCostModel::RemoveMachineFromRack(ResourceID_t res_id,
                                            EquivClass_t rack_ec,
                                            bool* rack_removed) {
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  CHECK_NOTNULL(machines_in_rack);
  ResourceID_t res_id_tmp = res_id;
  machines_in_rack->erase(res_id_tmp);
  if (machines_in_rack->size() == 0) {
    // The rack doesn't have any machines left. Delete it!
    // We have to delete empty racks because we're using the number
    // of racks to efficiently find if there's a rack on which a task has no
    // data.
    rack_to_machine_res_.erase(rack_ec);
    RemovePreferencesToRack(rack_ec);
    *rack_removed = true;
  } else {
    racks_with_spare_links_.insert(rack_ec);
    *rack_removed = false;
  }
  machine_to_rack_ec_.erase(res_id);
}

void QuincyCostModel::RemoveTask(TaskID_t task_id) {
  task_prefered_ecs_.erase(task_id);
  task_prefered_machines_.erase(task_id);
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

uint64_t QuincyCostModel::ComputeDataStatistics(
    const TaskDescriptor& td,
    unordered_map<ResourceID_t, uint64_t,
      boost::hash<boost::uuids::uuid>>* data_on_machines,
    unordered_map<EquivClass_t, uint64_t>* data_on_racks) {
  CHECK_NOTNULL(data_on_machines);
  CHECK_NOTNULL(data_on_racks);
  unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
                boost::hash<boost::uuids::uuid>> blocks_on_machines;
  unordered_map<EquivClass_t,
                unordered_map<uint64_t, uint64_t>> blocks_on_racks;
  DataLayerManagerInterface* data_layer =
    knowledge_base_->mutable_data_layer_manager();
  uint64_t input_size = 0;
  for (auto& dependency : td.dependencies()) {
    input_size += dependency.size();
    list<DataLocation> locations =
      data_layer->GetFileLocations(dependency.location());
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
    InsertIfNotPresent(data_on_machines, machine_blocks.first, data_on_machine);
  }
  for (auto& rack_blocks : blocks_on_racks) {
    uint64_t data_on_rack = 0;
    for (auto& block_size : rack_blocks.second) {
      data_on_rack += block_size.second;
    }
    InsertIfNotPresent(data_on_racks, rack_blocks.first, data_on_rack);
  }
  return input_size;
}

int64_t QuincyCostModel::ComputeTransferCostToMachine(uint64_t remote_data,
                                                      uint64_t data_on_rack) {
  return (FLAGS_quincy_tor_transfer_cost * data_on_rack +
    FLAGS_quincy_core_transfer_cost * (remote_data - data_on_rack)) /
    BYTES_TO_GB;
}

int64_t QuincyCostModel::ComputeTransferCostToRack(
    EquivClass_t ec,
    uint64_t input_size,
    uint64_t data_on_rack,
    const unordered_map<ResourceID_t, uint64_t,
      boost::hash<boost::uuids::uuid>>& data_on_machines) {
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, ec);
  int64_t cost_worst_machine = 0;
  for (auto& machine_res_id : *machines_in_rack) {
    const uint64_t* data_on_machine =
      FindOrNull(data_on_machines, machine_res_id);
    CHECK_NOTNULL(data_on_machine);
    int64_t cost_to_machine =
      ComputeTransferCostToMachine(input_size - *data_on_machine,
                                   data_on_rack - *data_on_machine);
    cost_worst_machine = max(cost_worst_machine, cost_to_machine);
  }
  return cost_worst_machine;
}

void QuincyCostModel::ConstructTaskPreferedSet(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  unordered_map<EquivClass_t, uint64_t> data_on_ecs;
  unordered_map<ResourceID_t, uint64_t,
                boost::hash<boost::uuids::uuid>> data_on_machines;
  // Compute the amount of data the task has on every machine and rack.
  uint64_t input_size =
    ComputeDataStatistics(td, &data_on_machines, &data_on_ecs);

  auto prefered_ecs = FindOrNull(task_prefered_ecs_, task_id);
  CHECK_NOTNULL(prefered_ecs);
  auto prefered_machines = FindOrNull(task_prefered_machines_, task_id);
  CHECK_NOTNULL(prefered_machines);

  for (auto& machine_data : data_on_machines) {
    uint64_t data_on_machine = machine_data.second;
    if (data_on_machine >=
        input_size * FLAGS_quincy_prefered_machine_data_fraction) {
      // Machine has more data than the required threshold => add it to
      // the prefered list.
      EquivClass_t* rack_ec =
        FindOrNull(machine_to_rack_ec_, machine_data.first);
      CHECK_NOTNULL(rack_ec);
      const uint64_t* data_on_rack = FindOrNull(data_on_ecs, *rack_ec);
      CHECK_NOTNULL(data_on_rack);
      int64_t transfer_cost =
        ComputeTransferCostToMachine(input_size - data_on_machine,
                                     *data_on_rack - data_on_machine);
      DataCost data_cost(data_on_machine, transfer_cost);
      CHECK(InsertIfNotPresent(prefered_machines, machine_data.first,
                               data_cost));
    }
  }
  int64_t worst_cluster_cost = 0;
  if (data_on_ecs.size() < rack_to_machine_res_.size()) {
    // There are racks on which we have no data.
    worst_cluster_cost = FLAGS_quincy_core_transfer_cost * input_size;
  }
  for (auto& rack_data : data_on_ecs) {
    if (rack_data.second >=
        input_size * FLAGS_quincy_prefered_rack_data_fraction) {
      // Rack has more data than the required threshold => add it to
      // the prefered list.
      int64_t transfer_cost =
        ComputeTransferCostToRack(rack_data.first, input_size, rack_data.second,
                                  data_on_machines);
      worst_cluster_cost = max(worst_cluster_cost, transfer_cost);
      DataCost data_cost(rack_data.second, transfer_cost);
      CHECK(InsertIfNotPresent(prefered_ecs, rack_data.first, data_cost));
    }
  }
  // Add transfer cost to the cluster aggregator.
  DataCost data_cost(input_size, worst_cluster_cost);
  CHECK(InsertIfNotPresent(prefered_ecs, cluster_aggregator_ec_, data_cost));
}

void QuincyCostModel::UpdateMachineBlocks(
    const DataLocation& location,
    unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
      boost::hash<boost::uuids::uuid>>* data_on_machines) {
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
  EquivClass_t* rack_ec =
    FindOrNull(machine_to_rack_ec_, location.machine_res_id_);
  auto rack_blocks =
    FindOrNull(*data_on_racks, *rack_ec);
  if (rack_blocks) {
    InsertOrUpdate(rack_blocks, location.block_id_, location.size_bytes_);
  } else {
    unordered_map<uint64_t, uint64_t> new_rack_blocks;
    InsertIfNotPresent(&new_rack_blocks, location.block_id_,
                       location.size_bytes_);
    InsertIfNotPresent(data_on_racks, *rack_ec, new_rack_blocks);
  }
}

void QuincyCostModel::UpdateTaskDataCosts(const TaskDescriptor& td,
                                          EquivClass_t ec_changed) {
  auto preferred_ecs = FindOrNull(task_prefered_ecs_, td.uid());
  int64_t cost_worst_machine = 0;
  // Get the worst cost to the racks that haven't changed.
  for (auto& ec_to_cost : *preferred_ecs) {
    if (ec_to_cost.first != cluster_aggregator_ec_ &&
        ec_to_cost.first != ec_changed) {
      cost_worst_machine =
        max(cost_worst_machine, ec_to_cost.second.transfer_cost_);
    }
  }
  UpdateTaskDataCostForRack(td, ec_changed);
  // Update cluster aggregator's cost.
  DataCost* cluster_data_cost =
    FindOrNull(*preferred_ecs, cluster_aggregator_ec_);
  cluster_data_cost->transfer_cost_ = cost_worst_machine;
}

void QuincyCostModel::UpdateTaskDataCostForRack(const TaskDescriptor& td,
                                                EquivClass_t rack_ec) {
  unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
                boost::hash<boost::uuids::uuid>> machines_blocks;
  unordered_map<uint64_t, uint64_t> rack_blocks;
  auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
  uint64_t input_size = 0;
  for (auto& dependency : td.dependencies()) {
    input_size += dependency.size();
    list<DataLocation> file_locations =
      knowledge_base_->mutable_data_layer_manager()->
      GetFileLocations(dependency.location());
    for (auto& data_location : file_locations) {
      // Only consider the blocks that are on a machine from the rack we're
      // updating.
      if (machines_in_rack->find(data_location.machine_res_id_) !=
          machines_in_rack->end()) {
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
  auto task_pref_machines = FindOrNull(task_prefered_machines_, td.uid());
  uint64_t worst_rack_cost = 0;
  for (auto& machine_res_id : *machines_in_rack) {
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
        UpdateTaskPreferedMachineList(td.uid(), input_size, machine_res_id,
                                      data_on_machine, transfer_cost,
                                      task_pref_machines);
      worst_rack_cost = max(worst_rack_cost, transfer_cost);
    } else {
      // No blocks on the machine.
      worst_rack_cost = ComputeTransferCostToMachine(input_size, data_on_rack);
    }
  }
  UpdateTaskPreferedRacksList(td.uid(), input_size, data_on_rack,
                              worst_rack_cost, rack_ec);
}

unordered_map<ResourceID_t, DataCost, boost::hash<boost::uuids::uuid>>*
  QuincyCostModel::UpdateTaskPreferedMachineList(
    TaskID_t task_id,
    uint64_t input_size,
    ResourceID_t machine_res_id,
    uint64_t data_on_machine,
    int64_t transfer_cost,
    unordered_map<ResourceID_t, DataCost, boost::hash<boost::uuids::uuid>>*
    task_pref_machines) {
  if (task_pref_machines) {
    // The task has preferences.
    DataCost* machine_data_cost =
      FindOrNull(*task_pref_machines, machine_res_id);
    if (machine_data_cost) {
      // The machine is already a prefered one.
      if (data_on_machine >= input_size * FLAGS_quincy_wait_time_factor) {
        machine_data_cost->transfer_cost_ = transfer_cost;
      } else {
        // The machine is not prefered anymore.
        task_pref_machines->erase(machine_res_id);
      }
    } else if (data_on_machine >=
               input_size * FLAGS_quincy_wait_time_factor) {
      // The machine has more data than the threshold => add it to the prefered
      // set.
      DataCost new_data_cost(data_on_machine, transfer_cost);
      InsertIfNotPresent(task_pref_machines, machine_res_id, new_data_cost);
    }
  } else if (data_on_machine >=
             input_size * FLAGS_quincy_wait_time_factor) {
    // The machine has more data than the threshold => create prefered set for
    // the task and add the machine to it.
    unordered_map<ResourceID_t, DataCost, boost::hash<boost::uuids::uuid>>
      new_pref_machines;
    DataCost new_data_cost(data_on_machine, transfer_cost);
    CHECK(InsertIfNotPresent(&task_prefered_machines_, task_id,
                             new_pref_machines));
    task_pref_machines = FindOrNull(task_prefered_machines_, task_id);
    InsertIfNotPresent(task_pref_machines, machine_res_id, new_data_cost);
  }
  return task_pref_machines;
}

void QuincyCostModel::UpdateTaskPreferedRacksList(
    TaskID_t task_id, uint64_t input_size, uint64_t data_on_rack,
    int64_t worst_rack_cost, EquivClass_t rack_ec) {
  auto pref_ecs = FindOrNull(task_prefered_ecs_, task_id);
  if (pref_ecs) {
    DataCost* rack_data_cost = FindOrNull(*pref_ecs, rack_ec);
    if (rack_data_cost) {
      if (data_on_rack >=
          input_size * FLAGS_quincy_prefered_rack_data_fraction) {
        // The rack keeps on being prefered. Update its DataCost.
        rack_data_cost->data_size_ = data_on_rack;
        rack_data_cost->transfer_cost_ = worst_rack_cost;
      } else {
        // The rack is no longer a prefered one.
        pref_ecs->erase(rack_ec);
      }
    } else {
      if (data_on_rack >=
          input_size * FLAGS_quincy_prefered_rack_data_fraction) {
        // The rack must be added to the prefered list.
        DataCost data_cost(data_on_rack, worst_rack_cost);
        InsertIfNotPresent(pref_ecs, rack_ec, data_cost);
      }
    }
  } else {
    if (data_on_rack >= input_size * FLAGS_quincy_prefered_rack_data_fraction) {
      unordered_map<EquivClass_t, DataCost> new_pref_ecs;
      DataCost data_cost(data_on_rack, worst_rack_cost);
      InsertIfNotPresent(&new_pref_ecs, rack_ec, data_cost);
      InsertIfNotPresent(&task_prefered_ecs_, task_id, new_pref_ecs);
    }
  }
}

}  // namespace firmament
