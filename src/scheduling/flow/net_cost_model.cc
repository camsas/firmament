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

#include "scheduling/flow/net_cost_model.h"

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"

DEFINE_uint64(max_multi_arcs, 10, "Maximum number of multi-arcs.");

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

NetCostModel::NetCostModel(shared_ptr<ResourceMap_t> resource_map,
                           shared_ptr<TaskMap_t> task_map,
                           shared_ptr<KnowledgeBase> knowledge_base)
  : resource_map_(resource_map), task_map_(task_map),
    knowledge_base_(knowledge_base) {
}

ArcDescriptor NetCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  return ArcDescriptor(2560000, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::TaskToResourceNode(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor NetCostModel::LeafResourceNodeToSink(ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor NetCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                       EquivClass_t ec) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor NetCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  uint64_t* required_net_rx_bw = FindOrNull(ec_rx_bw_requirement_, ec1);
  CHECK_NOTNULL(required_net_rx_bw);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  uint64_t available_net_rx_bw = rd.max_available_resources_below().net_rx_bw();
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index + 1;
  if (available_net_rx_bw < *required_net_rx_bw * ec_index) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  return ArcDescriptor(static_cast<int64_t>(ec_index) *
                       static_cast<int64_t>(*required_net_rx_bw) -
                       static_cast<int64_t>(available_net_rx_bw) + 1280000,
                       1ULL, 0ULL);
}

vector<EquivClass_t>* NetCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // Get the equivalence class for the task's required rx bw and label selectors.
  uint64_t* task_required_rx_bw = FindOrNull(task_rx_bw_requirement_, task_id);
  CHECK_NOTNULL(task_required_rx_bw);
  size_t rx_bw_selectors_hash = scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(rx_bw_selectors_hash, *task_required_rx_bw);
  EquivClass_t rx_bw_selectors_ec = static_cast<EquivClass_t>(rx_bw_selectors_hash);
  ecs->push_back(rx_bw_selectors_ec);
  InsertIfNotPresent(&ec_rx_bw_requirement_, rx_bw_selectors_ec, *task_required_rx_bw);
  InsertIfNotPresent(&ec_to_label_selectors, rx_bw_selectors_ec,
                     td_ptr->label_selectors());
  return ecs;
}

vector<ResourceID_t>* NetCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* machine_res = new vector<ResourceID_t>();
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec);
  if (machine_res_id) {
    machine_res->push_back(*machine_res_id);
  }
  return machine_res;
}

vector<ResourceID_t>* NetCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  return pref_res;
}

vector<EquivClass_t>* NetCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  uint64_t* required_net_rx_bw = FindOrNull(ec_rx_bw_requirement_, ec);
  if (required_net_rx_bw) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
      FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    // if EC is a rx bw EC then connect it to machine ECs.
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors))
        continue;
      uint64_t available_net_rx_bw =
        rd.max_available_resources_below().net_rx_bw();
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      for (uint64_t cur_rx_bw = *required_net_rx_bw;
           cur_rx_bw <= available_net_rx_bw && index < ecs_for_machine->size();
           cur_rx_bw += *required_net_rx_bw) {
        pref_ecs->push_back(ec_machines.second[index]);
        index++;
      }
    }
  }
  return pref_ecs;
}

void NetCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  // Keep track of the new machine
  CHECK(rd.type() == ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  vector<EquivClass_t> machine_ecs;
  for (uint64_t index = 0; index < FLAGS_max_multi_arcs; ++index) {
    EquivClass_t multi_machine_ec = GetMachineEC(rd.friendly_name(), index);
    machine_ecs.push_back(multi_machine_ec);
    CHECK(InsertIfNotPresent(&ec_to_index_, multi_machine_ec, index));
    CHECK(InsertIfNotPresent(&ec_to_machine_, multi_machine_ec, res_id));
  }
  CHECK(InsertIfNotPresent(&ecs_for_machines_, res_id, machine_ecs));
}

void NetCostModel::AddTask(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t required_net_rx_bw = td.resource_request().net_rx_bw();
  CHECK(InsertIfNotPresent(&task_rx_bw_requirement_, task_id,
                           required_net_rx_bw));
}

void NetCostModel::RemoveMachine(ResourceID_t res_id) {
  // vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  // CHECK_NOTNULL(ecs);
  // for (EquivClass_t& ec : *ecs) {
  //   CHECK_EQ(ec_to_machine_.erase(ec), 1);
  //   CHECK_EQ(ec_to_index_.erase(ec), 1);
  // }
  // CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
}

void NetCostModel::RemoveTask(TaskID_t task_id) {
  // CHECK_EQ(task_rx_bw_requirement_.erase(task_id), 1);
}

EquivClass_t NetCostModel::GetMachineEC(const string& machine_name,
                                        uint64_t ec_index) {
  uint64_t hash = HashString(machine_name);
  boost::hash_combine(hash, ec_index);
  return static_cast<EquivClass_t>(hash);
}

FlowGraphNode* NetCostModel::GatherStats(FlowGraphNode* accumulator,
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

  if (accumulator->type_ == FlowNodeType::MACHINE) {
    ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(accumulator->resource_id_,
                                                &latest_stats);
    if (have_sample) {
      rd_ptr->mutable_available_resources()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_tx_bw(
          rd_ptr->resource_capacity().net_tx_bw() -
          latest_stats.net_tx_bw());
      rd_ptr->mutable_available_resources()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_max_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
      rd_ptr->mutable_min_available_resources_below()->set_net_rx_bw(
          rd_ptr->resource_capacity().net_rx_bw() -
          latest_stats.net_rx_bw());
    }
  }

  return accumulator;
}

void NetCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* NetCostModel::UpdateStats(FlowGraphNode* accumulator,
                                         FlowGraphNode* other) {
  return accumulator;
}

}  // namespace firmament
