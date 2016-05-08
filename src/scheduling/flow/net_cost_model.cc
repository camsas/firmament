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

DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

NetCostModel::NetCostModel(shared_ptr<ResourceMap_t> resource_map,
                           shared_ptr<TaskMap_t> task_map,
                           shared_ptr<KnowledgeBase> knowledge_base)
  : resource_map_(resource_map), task_map_(task_map),
    knowledge_base_(knowledge_base) {
}

Cost_t NetCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 10000000000LL;
}

Cost_t NetCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t NetCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                            ResourceID_t resource_id) {
  const TaskDescriptor& td = GetTask(task_id);
  uint64_t required_net_bw = td.resource_request().net_bw();
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, resource_id);
  CHECK_NOTNULL(rs);
  const ResourceDescriptor& rd = rs->topology_node().resource_desc();
  CHECK_EQ(rd.type(), ResourceDescriptor::RESOURCE_MACHINE);
  uint64_t available_net_bw = rd.max_available_resources_below().net_bw();
  if (available_net_bw < required_net_bw) {
    return 10000000001LL;
  }
  return static_cast<int64_t>(required_net_bw) -
    static_cast<int64_t>(available_net_bw) + 10000000000LL;
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
  return pair<Cost_t, uint64_t>(0LL, 0ULL);
}

pair<Cost_t, uint64_t> NetCostModel::EquivClassToEquivClass(
    EquivClass_t ec1,
    EquivClass_t ec2) {
  return pair<Cost_t, uint64_t>(0LL, 0ULL);
}

vector<EquivClass_t>* NetCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  return NULL;
}

vector<ResourceID_t>* NetCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  return NULL;
}

vector<ResourceID_t>* NetCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* pref_res = new vector<ResourceID_t>();
  for (auto& machine_res_id : machines_) {
    ResourceStatus* rs = FindPtrOrNull(*resource_map_, machine_res_id);
    CHECK_NOTNULL(rs);
    const ResourceDescriptor& rd = rs->topology_node().resource_desc();
    uint64_t available_net_bw = rd.max_available_resources_below().net_bw();
    const TaskDescriptor& td = GetTask(task_id);
    uint64_t required_net_bw = td.resource_request().net_bw();
    if (available_net_bw >= required_net_bw) {
      pref_res->push_back(machine_res_id);
    }
  }
  return pref_res;
}

vector<EquivClass_t>* NetCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t ec) {
  return NULL;
}

void NetCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  // Keep track of the new machine
  CHECK(rtnd_ptr->resource_desc().type() ==
      ResourceDescriptor::RESOURCE_MACHINE);
  machines_.insert(ResourceIDFromString(rtnd_ptr->resource_desc().uuid()));
}

void NetCostModel::AddTask(TaskID_t task_id) {
}

void NetCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machines_.erase(res_id), 1);
}

void NetCostModel::RemoveTask(TaskID_t task_id) {
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
          (latest_stats.net_bw() / BYTES_TO_MB));
      rd_ptr->mutable_max_available_resources_below()->set_net_bw(
          rd_ptr->resource_capacity().net_bw() -
          (latest_stats.net_bw() / BYTES_TO_MB));
      rd_ptr->mutable_min_available_resources_below()->set_net_bw(
          rd_ptr->resource_capacity().net_bw() -
          (latest_stats.net_bw() / BYTES_TO_MB));
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
