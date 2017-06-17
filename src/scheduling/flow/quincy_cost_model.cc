/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
#include "scheduling/flow/cost_model_utils.h"

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
DEFINE_int64(quincy_positive_cost_offset, 2592000, "Value to offset costs so "
             "that they don't go negative. This value should be bigger than "
             "the runtime (in sec) of the longest task");
DEFINE_bool(quincy_no_scheduling_delay, false, "Offset cost to unscheduled "
            "aggregator so that tasks get scheduled as soon as possible");

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
ArcDescriptor QuincyCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  int64_t no_delay_offset = 0;
  if (FLAGS_quincy_no_scheduling_delay) {
    // XXX(ionel): HACK! In our simulations a task doesn't have more than 39GB
    // of input data. We offset the cost to the unscheduled aggregator for all
    // the tasks by MAX_TASK_INPUT_SIZE * FLAGS_quincy_core_transfer_cost to
    // force them to schedule as soon as possible.
    no_delay_offset = 39 * FLAGS_quincy_core_transfer_cost;
  }
  if (td.priority() == 1000) {
    // XXX(ionel): HACK! This forces synthetic tasks to be scheduled while
    // replaying a Google trace.
    return ArcDescriptor(
        100 + FLAGS_quincy_positive_cost_offset + no_delay_offset,
        1ULL, 0ULL);
  }
  int64_t total_unscheduled_time =
    static_cast<int64_t>(td.total_unscheduled_time());
  // Include current unscheduled wait period if it hasn't yet started.
  if (td.start_time() == 0 || td.start_time() < td.submit_time()) {
    total_unscheduled_time +=
      static_cast<int64_t>(time_manager_->GetCurrentTimestamp()) -
      static_cast<int64_t>(td.submit_time());
  }
  return ArcDescriptor(
      static_cast<Cost_t>(total_unscheduled_time *
                          FLAGS_quincy_wait_time_factor /
                          static_cast<int64_t>(MICROSECONDS_IN_SECOND) +
                          FLAGS_quincy_positive_cost_offset + no_delay_offset),
      1ULL, 0ULL);
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
ArcDescriptor QuincyCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor QuincyCostModel::TaskToResourceNode(TaskID_t task_id,
                                                  ResourceID_t resource_id) {
  auto machines_data = FindOrNull(task_preferred_machines_, task_id);
  if (machines_data) {
    Cost_t* transfer_cost = FindOrNull(*machines_data, resource_id);
    if (transfer_cost) {
      return ArcDescriptor(*transfer_cost + FLAGS_quincy_positive_cost_offset,
                           1ULL, 0ULL);
    } else {
      // The machine is not a preferred one.
      return ArcDescriptor(
          GetTransferCostToNotPreferredRes(task_id, resource_id) +
          FLAGS_quincy_positive_cost_offset, 1ULL, 0ULL);
    }
  } else {
    // The task doesn't have any preferred machines.
    return ArcDescriptor(
        GetTransferCostToNotPreferredRes(task_id, resource_id) +
        FLAGS_quincy_positive_cost_offset, 1ULL, 0ULL);
  }
}

Cost_t QuincyCostModel::GetTransferCostToNotPreferredRes(
    TaskID_t task_id,
    ResourceID_t res_id) {
  pair<ResourceID_t, Cost_t>* machine_transfer_cost =
    FindOrNull(task_running_arcs_, task_id);
  if (!machine_transfer_cost || machine_transfer_cost->first != res_id) {
    // The running arc did not exist previously or was pointing to a
    // different resource.
    TaskDescriptor* td_ptr = GetMutableTask(task_id);
    ResourceID_t machine_res_id =
      MachineResIDForResource(resource_map_, res_id);
    uint64_t data_on_rack = 0;
    uint64_t data_on_machine = 0;
    uint64_t input_size =
      ComputeDataStatsForMachine(td_ptr, machine_res_id, &data_on_rack,
                                 &data_on_machine);
    Cost_t transfer_cost =
      ComputeTransferCostToMachine(input_size - data_on_machine,
                                   data_on_rack - data_on_machine);
    // Cache the transfer cost.
    InsertOrUpdate(&task_running_arcs_, task_id,
                   pair<ResourceID_t, Cost_t>(res_id, transfer_cost));
    return transfer_cost;
  } else {
    return machine_transfer_cost->second;
  }
}

ArcDescriptor QuincyCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  // Cost between resource nodes is always 0.
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor QuincyCostModel::LeafResourceNodeToSink(
    ResourceID_t resource_id) {
  // The cost from the resource leaf to the sink is 0.
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor QuincyCostModel::TaskContinuation(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  ResourceID_t pu_res_id = ResourceIDFromString(td.scheduled_to_resource());
  ResourceID_t machine_res_id =
    MachineResIDForResource(resource_map_, pu_res_id);
  ArcDescriptor arc_cost_cap = TaskToResourceNode(task_id, machine_res_id);
  // NOTE: total_run_time only includes the time of previous runs. We need
  // to include the current run time as well in order for the continuation
  // cost to be correct.
  CHECK_GE(time_manager_->GetCurrentTimestamp(), td.start_time());
  uint64_t task_executed_for;
  task_executed_for =
    (td.total_run_time() + time_manager_->GetCurrentTimestamp() -
     td.start_time()) / MICROSECONDS_IN_SECOND;
  // arc_cost_cap.cost_ corresponds to d* and total_running_time corresponds
  // to p* in the Quincy paper.
  // NOTE: We don't have to offset the cost because the arc_cost_cap.cost_ is
  // already offsetted.
  return ArcDescriptor(arc_cost_cap.cost_ -
                       static_cast<Cost_t>(task_executed_for),
                       1ULL, 0ULL);
}

ArcDescriptor QuincyCostModel::TaskPreemption(TaskID_t task_id) {
  // NOTE: We don't have to offset the cost because TaskToUnscheduledAgg
  // already does it.
  return ArcDescriptor(TaskToUnscheduledAgg(task_id).cost_, 1ULL, 0ULL);
}

ArcDescriptor QuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                          EquivClass_t ec) {
  auto ec_costs = FindOrNull(task_preferred_ecs_, task_id);
  CHECK_NOTNULL(ec_costs);
  Cost_t* transfer_cost = FindOrNull(*ec_costs, ec);
  CHECK_NOTNULL(transfer_cost);
  return ArcDescriptor(*transfer_cost + FLAGS_quincy_positive_cost_offset,
                       1ULL, 0ULL);
}

ArcDescriptor QuincyCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  CHECK_NE(ec, cluster_aggregator_ec_);
  uint64_t capacity = GetNumSchedulableSlots(res_id);
  // Cost of arcs from rack aggregators are always zero.
  return ArcDescriptor(0LL, capacity, 0ULL);
}

ArcDescriptor QuincyCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  if (ec1 == cluster_aggregator_ec_) {
    // The other equivalence class must be a rack aggregator.
    const auto& machines_res_id = data_layer_manager_->GetMachinesInRack(ec2);
    uint64_t capacity = 0;
    for (const auto& machine_res_id : machines_res_id) {
      capacity += GetNumSchedulableSlots(machine_res_id);
    }
    return ArcDescriptor(0LL, capacity, 0ULL);
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
        UpdateTaskCosts(id_td.second, rack_ec, false);
      }
    }
  }
}

void QuincyCostModel::AddTask(TaskID_t task_id) {
  CHECK(InsertIfNotPresent(
      &task_preferred_ecs_, task_id, unordered_map<EquivClass_t, Cost_t>()));
  CHECK(InsertIfNotPresent(
      &task_preferred_machines_, task_id,
      unordered_map<ResourceID_t, Cost_t, boost::hash<ResourceID_t>>()));
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
        UpdateTaskCosts(id_td.second, rack_ec, rack_removed);
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
          static_cast<uint64_t>(
              accumulator->rd_ptr_->current_running_tasks_size()));
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
    TaskDescriptor* td_ptr,
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
  for (RepeatedPtrField<ReferenceDescriptor>::pointer_iterator
         dependency_it = td_ptr->mutable_dependencies()->pointer_begin();
       dependency_it != td_ptr->mutable_dependencies()->pointer_end();
       ++dependency_it) {
    auto& dependency = *dependency_it;
    string location = dependency->location();
    if (dependency->size() == 0) {
      dependency->set_size(data_layer_manager_->GetFileSize(location));
    }
    input_size += dependency->size();
    list<DataLocation> locations;
    data_layer_manager_->GetFileLocations(location, &locations);
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
    TaskDescriptor* td_ptr, ResourceID_t machine_res_id,
    uint64_t* data_on_rack, uint64_t* data_on_machine) {
  unordered_set<uint64_t> machine_block_ids;
  unordered_set<uint64_t> rack_block_ids;
  EquivClass_t rack_ec = data_layer_manager_->GetRackForMachine(machine_res_id);
  uint64_t input_size = 0;
  for (RepeatedPtrField<ReferenceDescriptor>::pointer_iterator
         dependency_it = td_ptr->mutable_dependencies()->pointer_begin();
       dependency_it != td_ptr->mutable_dependencies()->pointer_end();
       ++dependency_it) {
    auto& dependency = *dependency_it;
    string location = dependency->location();
    if (dependency->size() == 0) {
      dependency->set_size(data_layer_manager_->GetFileSize(location));
    }
    input_size += dependency->size();
    list<DataLocation> locations;
    data_layer_manager_->GetFileLocations(location, &locations);
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

Cost_t QuincyCostModel::ComputeTransferCostToMachine(uint64_t remote_data,
                                                     uint64_t data_on_rack) {
  CHECK_GE(remote_data, data_on_rack);
  return (FLAGS_quincy_tor_transfer_cost * static_cast<int64_t>(data_on_rack) +
          FLAGS_quincy_core_transfer_cost *
          static_cast<int64_t>(remote_data - data_on_rack)) /
    static_cast<int64_t>(BYTES_TO_GB);
}

Cost_t QuincyCostModel::ComputeTransferCostToRack(
    EquivClass_t ec,
    uint64_t input_size,
    uint64_t data_on_rack,
    const unordered_map<ResourceID_t, uint64_t,
      boost::hash<ResourceID_t>>& data_on_machines) {
  const auto& machines_in_rack = data_layer_manager_->GetMachinesInRack(ec);
  Cost_t cost_worst_machine = INT64_MIN;
  for (const auto& machine_res_id : machines_in_rack) {
    const uint64_t* data_on_machine_ptr =
      FindOrNull(data_on_machines, machine_res_id);
    uint64_t data_on_machine = 0;
    if (data_on_machine_ptr) {
      data_on_machine = *data_on_machine_ptr;
    }
    Cost_t cost_to_machine =
      ComputeTransferCostToMachine(input_size - data_on_machine,
                                   data_on_rack - data_on_machine);
    cost_worst_machine = max(cost_worst_machine, cost_to_machine);
  }
  CHECK_GE(cost_worst_machine, 0);
  return cost_worst_machine;
}

void QuincyCostModel::ConstructTaskPreferredSet(TaskID_t task_id) {
  TaskDescriptor* td_ptr = GetMutableTask(task_id);
  unordered_map<EquivClass_t, uint64_t> data_on_ecs;
  unordered_map<ResourceID_t, uint64_t,
                boost::hash<ResourceID_t>> data_on_machines;
  // Compute the amount of data the task has on every machine and rack.
  uint64_t input_size =
    ComputeClusterDataStatistics(td_ptr, &data_on_machines, &data_on_ecs);

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
      Cost_t transfer_cost =
        ComputeTransferCostToMachine(input_size - data_on_machine,
                                     *data_on_rack - data_on_machine);
      CHECK(InsertIfNotPresent(preferred_machines, machine_data.first,
                               transfer_cost));
      best_machine_cost = min(best_machine_cost, transfer_cost);
    }
  }
  Cost_t worst_cluster_cost = INT64_MIN;
  Cost_t best_rack_cost = INT64_MAX;
  if (data_on_ecs.size() < data_layer_manager_->GetNumRacks()) {
    // There are racks on which we have no data.
    worst_cluster_cost = FLAGS_quincy_core_transfer_cost *
      static_cast<int64_t>(input_size) / static_cast<int64_t>(BYTES_TO_GB);
    best_rack_cost = min(best_rack_cost, worst_cluster_cost);
  }
  for (auto& rack_data : data_on_ecs) {
    Cost_t transfer_cost =
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
                                    TaskToUnscheduledAgg(td_ptr->uid()).cost_,
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

void QuincyCostModel::UpdateTaskCosts(TaskDescriptor* td_ptr,
                                      EquivClass_t ec_changed,
                                      bool rack_removed) {
  auto preferred_ecs = FindOrNull(task_preferred_ecs_, td_ptr->uid());
  // Every task should at least have an arc to the cluster aggregator
  // equivalence class.
  CHECK_NOTNULL(preferred_ecs);
  // TODO(ionel): We don't correctly update the cost for the case in which we
  // remove the worst machine. To correctly handle it we would have to revisit
  // all the ECs and figure out the new worst machine. However, this would be
  // too expensive.
  Cost_t* prev_worst_cost = FindOrNull(*preferred_ecs, cluster_aggregator_ec_);
  CHECK_NOTNULL(prev_worst_cost);
  Cost_t cost_worst_machine = *prev_worst_cost;
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
      max(cost_worst_machine, UpdateTaskCostForRack(td_ptr, ec_changed));
  } else {
    preferred_ecs->erase(ec_changed);
  }
  // Update cluster aggregator's cost.
  CHECK_GE(cost_worst_machine, 0);
  InsertOrUpdate(preferred_ecs, cluster_aggregator_ec_, cost_worst_machine);
}

Cost_t QuincyCostModel::UpdateTaskCostForRack(TaskDescriptor* td_ptr,
                                              EquivClass_t rack_ec) {
  unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
                boost::hash<ResourceID_t>> machines_blocks;
  unordered_map<uint64_t, uint64_t> rack_blocks;
  const auto& machines_in_rack =
    data_layer_manager_->GetMachinesInRack(rack_ec);
  uint64_t input_size = 0;
  for (RepeatedPtrField<ReferenceDescriptor>::pointer_iterator
         dependency_it = td_ptr->mutable_dependencies()->pointer_begin();
       dependency_it != td_ptr->mutable_dependencies()->pointer_end();
       ++dependency_it) {
    auto& dependency = *dependency_it;
    string location = dependency->location();
    if (dependency->size() == 0) {
      dependency->set_size(data_layer_manager_->GetFileSize(location));
    }
    input_size += dependency->size();
    list<DataLocation> file_locations;
    data_layer_manager_->GetFileLocations(location, &file_locations);
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
  auto task_pref_machines = FindOrNull(task_preferred_machines_, td_ptr->uid());
  Cost_t worst_rack_cost = INT64_MIN;
  for (const auto& machine_res_id : machines_in_rack) {
    auto machine_blocks = FindOrNull(machines_blocks, machine_res_id);
    if (machine_blocks) {
      // Compute the amount of data the task has on the machine.
      uint64_t data_on_machine = 0;
      for (auto& machine_block_size : *machine_blocks) {
        data_on_machine += machine_block_size.second;
      }
      Cost_t transfer_cost =
        ComputeTransferCostToMachine(input_size - data_on_machine,
                                     data_on_rack - data_on_machine);
      task_pref_machines =
        UpdateTaskPreferredMachineList(td_ptr->uid(), input_size, machine_res_id,
                                       data_on_machine, transfer_cost,
                                       task_pref_machines);
      worst_rack_cost = max(worst_rack_cost, transfer_cost);
    } else {
      // No blocks on the machine.
      worst_rack_cost = ComputeTransferCostToMachine(input_size, data_on_rack);
    }
  }
  UpdateTaskPreferredRacksList(td_ptr->uid(), input_size, data_on_rack,
                               worst_rack_cost, rack_ec);
  return worst_rack_cost;
}

unordered_map<ResourceID_t, Cost_t, boost::hash<ResourceID_t>>*
  QuincyCostModel::UpdateTaskPreferredMachineList(
    TaskID_t task_id,
    uint64_t input_size,
    ResourceID_t machine_res_id,
    uint64_t data_on_machine,
    Cost_t transfer_cost,
    unordered_map<ResourceID_t, Cost_t, boost::hash<ResourceID_t>>*
    task_pref_machines) {
  if (task_pref_machines) {
    // The task has preferences.
    Cost_t* machine_transfer_cost =
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
    unordered_map<ResourceID_t, Cost_t, boost::hash<ResourceID_t>>
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
    Cost_t worst_rack_cost, EquivClass_t rack_ec) {
  auto pref_ecs = FindOrNull(task_preferred_ecs_, task_id);
  if (pref_ecs) {
    Cost_t* rack_cost = FindOrNull(*pref_ecs, rack_ec);
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
      unordered_map<EquivClass_t, Cost_t> new_pref_ecs;
      InsertIfNotPresent(&new_pref_ecs, rack_ec, worst_rack_cost);
      InsertIfNotPresent(&task_preferred_ecs_, task_id, new_pref_ecs);
    }
  }
}

}  // namespace firmament
