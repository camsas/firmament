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

#include "scheduling/flow/cpu_mem_cost_model.h"
#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/flow_graph_manager.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/label_utils.h"

DEFINE_uint64(max_multi_arcs_for_cpu, 50, "Maximum number of multi-arcs.");

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

CpuMemCostModel::CpuMemCostModel(shared_ptr<ResourceMap_t> resource_map,
                                 shared_ptr<TaskMap_t> task_map,
                                 shared_ptr<KnowledgeBase> knowledge_base)
    : resource_map_(resource_map),
      task_map_(task_map),
      knowledge_base_(knowledge_base) {}

void CpuMemCostModel::AccumulateResourceStats(ResourceDescriptor* accumulator,
                                              ResourceDescriptor* other) {
  // Track the aggregate available resources below the machine node
  ResourceVector* acc_avail = accumulator->mutable_available_resources();
  ResourceVector* other_avail = other->mutable_available_resources();
  acc_avail->set_cpu_cores(acc_avail->cpu_cores() + other_avail->cpu_cores());
  // Running/idle task count
  accumulator->set_num_running_tasks_below(
      accumulator->num_running_tasks_below() +
      other->num_running_tasks_below());
  accumulator->set_num_slots_below(accumulator->num_slots_below() +
                                   other->num_slots_below());
}

ArcDescriptor CpuMemCostModel::TaskToUnscheduledAgg(TaskID_t task_id) {
  return ArcDescriptor(2560000, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::UnscheduledAggToSink(JobID_t job_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::TaskToResourceNode(TaskID_t task_id,
                                                  ResourceID_t resource_id) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::ResourceNodeToResourceNode(
    const ResourceDescriptor& source, const ResourceDescriptor& destination) {
  return ArcDescriptor(0LL, CapacityFromResNodeToParent(destination), 0ULL);
}

ArcDescriptor CpuMemCostModel::LeafResourceNodeToSink(
    ResourceID_t resource_id) {
  return ArcDescriptor(0LL, FLAGS_max_tasks_per_pu, 0ULL);
}

ArcDescriptor CpuMemCostModel::TaskContinuation(TaskID_t task_id) {
  // TODO(shivramsrivastava): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::TaskPreemption(TaskID_t task_id) {
  // TODO(shivramsrivastava): Implement before running with preemption enabled.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                          EquivClass_t ec) {
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::EquivClassToResourceNode(EquivClass_t ec,
                                                        ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return ArcDescriptor(0LL, 1ULL, 0ULL);
}

ArcDescriptor CpuMemCostModel::EquivClassToEquivClass(EquivClass_t ec1,
                                                      EquivClass_t ec2) {
  CpuMemCostVector_t* resource_request =
      FindOrNull(ec_resource_requirement_, ec1);
  CHECK_NOTNULL(resource_request);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  CpuMemCostVector_t available_resources;
  available_resources.cpu_cores_ =
      static_cast<uint64_t>(rd.available_resources().cpu_cores());
  available_resources.ram_cap_ = rd.available_resources().ram_cap();
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index;
  if ((available_resources.cpu_cores_ <
       resource_request->cpu_cores_ * ec_index) ||
      (available_resources.ram_cap_ < resource_request->ram_cap_ * ec_index)) {
    return ArcDescriptor(0LL, 0ULL, 0ULL);
  }
  available_resources.cpu_cores_ =
      available_resources.cpu_cores_ - ec_index * resource_request->cpu_cores_;
  available_resources.ram_cap_ =
      available_resources.ram_cap_ - ec_index * resource_request->ram_cap_;
  int64_t cpu_cost =
      ((rd.resource_capacity().cpu_cores() - available_resources.cpu_cores_) /
       rd.resource_capacity().cpu_cores()) *
      omega_;
  int64_t ram_cost =
      ((rd.resource_capacity().ram_cap() - available_resources.ram_cap_) /
       (float)rd.resource_capacity().ram_cap()) *
      omega_;
  int64_t cost = cpu_cost + ram_cost;
  return ArcDescriptor(cost, 1ULL, 0ULL);
}

vector<EquivClass_t>* CpuMemCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  // Get the equivalence class for the resource request: cpu and memory
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  CpuMemCostVector_t* task_resource_request =
      FindOrNull(task_resource_requirement_, task_id);
  CHECK_NOTNULL(task_resource_request);
  size_t resource_req_selectors_hash =
      scheduler::HashSelectors(td_ptr->label_selectors());
  boost::hash_combine(resource_req_selectors_hash,
                      to_string(task_resource_request->cpu_cores_) + "cpumem" +
                          to_string(task_resource_request->ram_cap_));
  EquivClass_t resource_request_ec =
      static_cast<EquivClass_t>(resource_req_selectors_hash);
  ecs->push_back(resource_request_ec);
  InsertIfNotPresent(&ec_resource_requirement_, resource_request_ec,
                     *task_resource_request);
  InsertIfNotPresent(&ec_to_label_selectors, resource_request_ec,
                     td_ptr->label_selectors());
  return ecs;
}

vector<ResourceID_t>* CpuMemCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* machine_res = new vector<ResourceID_t>();
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec);
  if (machine_res_id) {
    machine_res->push_back(*machine_res_id);
  }
  return machine_res;
}

vector<ResourceID_t>* CpuMemCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  return pref_res;
}

vector<EquivClass_t>* CpuMemCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  vector<EquivClass_t>* pref_ecs = new vector<EquivClass_t>();
  CpuMemCostVector_t* task_resource_request =
      FindOrNull(ec_resource_requirement_, ec);
  if (task_resource_request) {
    const RepeatedPtrField<LabelSelector>* label_selectors =
        FindOrNull(ec_to_label_selectors, ec);
    CHECK_NOTNULL(label_selectors);
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      if (!scheduler::SatisfiesLabelSelectors(rd, *label_selectors)) continue;
      CpuMemCostVector_t available_resources;
      available_resources.cpu_cores_ =
          static_cast<uint32_t>(rd.available_resources().cpu_cores());
      available_resources.ram_cap_ =
          static_cast<uint32_t>(rd.available_resources().ram_cap());
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
          FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      CpuMemCostVector_t cur_resource;
      for (cur_resource = *task_resource_request;
           cur_resource.cpu_cores_ <= available_resources.cpu_cores_ &&
           cur_resource.ram_cap_ <= available_resources.ram_cap_ &&
           index < ecs_for_machine->size();
           cur_resource.cpu_cores_ += task_resource_request->cpu_cores_,
          cur_resource.ram_cap_ += task_resource_request->ram_cap_, index++) {
        pref_ecs->push_back(ec_machines.second[index]);
      }
    }
  }
  return pref_ecs;
}

void CpuMemCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  // Keep track of the new machine
  CHECK(rd.type() == ResourceDescriptor::RESOURCE_MACHINE);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  vector<EquivClass_t> machine_ecs;
  for (uint64_t index = 0; index < FLAGS_max_multi_arcs_for_cpu; ++index) {
    EquivClass_t multi_machine_ec = GetMachineEC(rd.friendly_name(), index);
    machine_ecs.push_back(multi_machine_ec);
    CHECK(InsertIfNotPresent(&ec_to_index_, multi_machine_ec, index));
    CHECK(InsertIfNotPresent(&ec_to_machine_, multi_machine_ec, res_id));
  }
  CHECK(InsertIfNotPresent(&ecs_for_machines_, res_id, machine_ecs));
}

void CpuMemCostModel::AddTask(TaskID_t task_id) {
  const TaskDescriptor& td = GetTask(task_id);
  CpuMemCostVector_t resource_request;
  resource_request.cpu_cores_ =
      static_cast<uint32_t>(td.resource_request().cpu_cores());
  resource_request.ram_cap_ =
      static_cast<uint32_t>(td.resource_request().ram_cap());
  CHECK(InsertIfNotPresent(&task_resource_requirement_, task_id,
                           resource_request));
}

void CpuMemCostModel::RemoveMachine(ResourceID_t res_id) {
  vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  CHECK_NOTNULL(ecs);
  for (EquivClass_t& ec : *ecs) {
    CHECK_EQ(ec_to_machine_.erase(ec), 1);
    CHECK_EQ(ec_to_index_.erase(ec), 1);
  }
  CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
}

void CpuMemCostModel::RemoveTask(TaskID_t task_id) {
  CHECK_EQ(task_resource_requirement_.erase(task_id), 1);
}

EquivClass_t CpuMemCostModel::GetMachineEC(const string& machine_name,
                                           uint64_t ec_index) {
  uint64_t hash = HashString(machine_name);
  boost::hash_combine(hash, ec_index);
  return static_cast<EquivClass_t>(hash);
}

FlowGraphNode* CpuMemCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  if (!accumulator->IsResourceNode()) {
    return accumulator;
  }

  if (accumulator->type_ == FlowNodeType::COORDINATOR) {
    // We're not allowed to scheduled tasks via the cluster aggregator in CoCo.
    // There's no point to update its state.
    return accumulator;
  }

  ResourceDescriptor* rd_ptr = accumulator->rd_ptr_;
  CHECK_NOTNULL(rd_ptr);
  ResourceID_t machine_res_id =
      MachineResIDForResource(accumulator->resource_id_);
  if (accumulator->type_ == FlowNodeType::PU) {
    CHECK(other->resource_id_.is_nil());
    ResourceStats latest_stats;
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(machine_res_id,
                                                                 &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating PU " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      // Get the CPU stats for this PU
      string label = rd_ptr->friendly_name();
      uint64_t idx = label.find("PU #");
      if (idx != string::npos) {
        string core_id_substr = label.substr(idx + 4, label.size() - idx - 4);
        uint32_t core_id = strtoul(core_id_substr.c_str(), 0, 10);
        float available_cpu_cores =
            latest_stats.cpus_stats(core_id).cpu_capacity() *
            (1.0 - latest_stats.cpus_stats(core_id).cpu_utilization());
        rd_ptr->mutable_available_resources()->set_cpu_cores(
            available_cpu_cores);
      }
      // Running/idle task count
      rd_ptr->set_num_running_tasks_below(rd_ptr->current_running_tasks_size());
      rd_ptr->set_num_slots_below(FLAGS_max_tasks_per_pu);
      return accumulator;
    }
  } else if (accumulator->type_ == FlowNodeType::MACHINE) {
    // Grab the latest available resource sample from the machine
    ResourceStats latest_stats;
    // Take the most recent sample for now
    bool have_sample = knowledge_base_->GetLatestStatsForMachine(
        accumulator->resource_id_, &latest_stats);
    if (have_sample) {
      VLOG(2) << "Updating machine " << accumulator->resource_id_ << "'s "
              << "resource stats!";
      rd_ptr->mutable_available_resources()->set_ram_cap(
          latest_stats.mem_capacity() * (1.0 - latest_stats.mem_utilization()));
    }
    if (accumulator->rd_ptr_ && other->rd_ptr_) {
      AccumulateResourceStats(accumulator->rd_ptr_, other->rd_ptr_);
    }
  }
  return accumulator;
}

void CpuMemCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (!accumulator->IsResourceNode()) {
    return;
  }
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
  accumulator->rd_ptr_->clear_available_resources();
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* CpuMemCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return accumulator;
}

ResourceID_t CpuMemCostModel::MachineResIDForResource(ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  ResourceTopologyNodeDescriptor* rtnd = rs->mutable_topology_node();
  while (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_MACHINE) {
    if (rtnd->parent_id().empty()) {
      LOG(FATAL) << "Non-machine resource " << rtnd->resource_desc().uuid()
                 << " has no parent!";
    }
    rs = FindPtrOrNull(*resource_map_, ResourceIDFromString(rtnd->parent_id()));
    rtnd = rs->mutable_topology_node();
  }
  return ResourceIDFromString(rtnd->resource_desc().uuid());
}

}  // namespace firmament
