// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "scheduling/flow/net_cost_model.h"

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
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

Cost_t NetCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 2500;
}

Cost_t NetCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t NetCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  return 0LL;
}

Cost_t NetCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return 0LL;
}

Cost_t NetCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t NetCostModel::TaskContinuationCost(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return 0LL;
}

Cost_t NetCostModel::TaskPreemptionCost(TaskID_t task_id) {
  // TODO(ionel): Implement before running with preemption enabled.
  return 0LL;
}

Cost_t NetCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                EquivClass_t ec) {
  return 0LL;
}

pair<Cost_t, uint64_t> NetCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  // The arcs between ECs an machine can only carry unit flow.
  return pair<Cost_t, uint64_t>(0LL, 1ULL);
}

pair<Cost_t, uint64_t> NetCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  uint64_t* required_net_bw = FindOrNull(ec_bw_requirement_, ec1);
  CHECK_NOTNULL(required_net_bw);
  ResourceID_t* machine_res_id = FindOrNull(ec_to_machine_, ec2);
  CHECK_NOTNULL(machine_res_id);
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, *machine_res_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  uint64_t available_net_bw = rd.max_available_resources_below().net_bw();
  uint64_t* index = FindOrNull(ec_to_index_, ec2);
  CHECK_NOTNULL(index);
  uint64_t ec_index = *index + 1;
  if (available_net_bw < *required_net_bw * ec_index) {
    return pair<Cost_t, uint64_t>(0LL, 0ULL);
  }
  return pair<Cost_t, uint64_t>(static_cast<int64_t>(ec_index) *
                                static_cast<int64_t>(*required_net_bw) -
                                static_cast<int64_t>(available_net_bw) + 1250LL,
                                1ULL);
}

vector<EquivClass_t>* NetCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* ecs = new vector<EquivClass_t>();
  // Get the equivalence class for the task's required bw.
  uint64_t* task_required_bw = FindOrNull(task_bw_requirement_, task_id);
  CHECK_NOTNULL(task_required_bw);
  EquivClass_t bw_ec = static_cast<EquivClass_t>(HashInt(*task_required_bw));
  ecs->push_back(bw_ec);
  InsertIfNotPresent(&ec_bw_requirement_, bw_ec, *task_required_bw);
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
  uint64_t* required_net_bw = FindOrNull(ec_bw_requirement_, ec);
  if (required_net_bw) {
    // if EC is a bw EC then connect it to machine ECs.
    for (auto& ec_machines : ecs_for_machines_) {
      ResourceStatus* rs = FindPtrOrNull(*resource_map_, ec_machines.first);
      CHECK_NOTNULL(rs);
      const ResourceDescriptor& rd = rs->topology_node().resource_desc();
      uint64_t available_net_bw = rd.max_available_resources_below().net_bw();
      ResourceID_t res_id = ResourceIDFromString(rd.uuid());
      vector<EquivClass_t>* ecs_for_machine =
        FindOrNull(ecs_for_machines_, res_id);
      CHECK_NOTNULL(ecs_for_machine);
      uint64_t index = 0;
      for (uint64_t cur_bw = *required_net_bw;
           cur_bw <= available_net_bw && index < ecs_for_machine->size();
           cur_bw += *required_net_bw) {
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
  uint64_t required_net_bw = td.resource_request().net_bw();
  CHECK(InsertIfNotPresent(&task_bw_requirement_, task_id, required_net_bw));
}

void NetCostModel::RemoveMachine(ResourceID_t res_id) {
  vector<EquivClass_t>* ecs = FindOrNull(ecs_for_machines_, res_id);
  CHECK_NOTNULL(ecs);
  for (EquivClass_t& ec : *ecs) {
    CHECK_EQ(ec_to_machine_.erase(ec), 1);
    CHECK_EQ(ec_to_index_.erase(ec), 1);
  }
  CHECK_EQ(ecs_for_machines_.erase(res_id), 1);
}

void NetCostModel::RemoveTask(TaskID_t task_id) {
  CHECK_EQ(task_bw_requirement_.erase(task_id), 1);
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
    MachinePerfStatisticsSample latest_stats;
    // Take the most recent sample for now
    bool have_sample =
      knowledge_base_->GetLatestStatsForMachine(accumulator->resource_id_,
                                                &latest_stats);
    if (have_sample) {
      rd_ptr->mutable_available_resources()->set_net_bw(
          rd_ptr->resource_capacity().net_bw() -
          latest_stats.net_bw() / BYTES_TO_MB);
      rd_ptr->mutable_max_available_resources_below()->set_net_bw(
          rd_ptr->resource_capacity().net_bw() -
          latest_stats.net_bw() / BYTES_TO_MB);
      rd_ptr->mutable_min_available_resources_below()->set_net_bw(
          rd_ptr->resource_capacity().net_bw() -
          latest_stats.net_bw() / BYTES_TO_MB);
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
