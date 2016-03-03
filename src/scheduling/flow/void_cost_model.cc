// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/void_cost_model.h"

#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

VoidCostModel::VoidCostModel(shared_ptr<ResourceMap_t> resource_map,
                             shared_ptr<TaskMap_t> task_map)
    : resource_map_(resource_map), task_map_(task_map) {
}

Cost_t VoidCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                             ResourceID_t resource_id) {
  return 0LL;
}

Cost_t VoidCostModel::ClusterAggToResourceNodeCost(ResourceID_t target) {
  return 0LL;
}

Cost_t VoidCostModel::ResourceNodeToResourceNodeCost(
    const ResourceDescriptor& source,
    const ResourceDescriptor& destination) {
  return 0LL;
}

Cost_t VoidCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t VoidCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                 EquivClass_t tec) {
  return 0LL;
}

pair<Cost_t, uint64_t> VoidCostModel::EquivClassToResourceNode(
    EquivClass_t tec,
    ResourceID_t res_id) {
  ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
  CHECK_NOTNULL(rs);
  uint64_t num_free_slots = rs->descriptor().num_slots_below() -
    rs->descriptor().num_running_tasks_below();
  return pair<Cost_t, uint64_t>(0LL, num_free_slots);
}

Cost_t VoidCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                             EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* VoidCostModel::GetTaskEquivClasses(TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // We have one task EC per program.
  // The ID of the aggregator is the hash of the command line.
  // We need this EC even in the void cost model in order to make the EC
  // statistics view on the web UI work.
  EquivClass_t task_agg =
    static_cast<EquivClass_t>(HashCommandLine(*td_ptr));
  equiv_classes->push_back(task_agg);
  equiv_classes->push_back(task_id);
  return equiv_classes;
}

vector<EquivClass_t>* VoidCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  return NULL;
}

vector<ResourceID_t>* VoidCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  for (auto& res_id_rtnd : machine_to_rtnd_) {
    prefered_res->push_back(res_id_rtnd.first);
  }
  return prefered_res;
}

vector<ResourceID_t>* VoidCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  return NULL;
}

vector<EquivClass_t>* VoidCostModel::GetEquivClassToEquivClassesArcs(
    EquivClass_t tec) {
  return NULL;
}

void VoidCostModel::AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_EQ(rtnd_ptr->resource_desc().type(),
           ResourceDescriptor::RESOURCE_MACHINE);
  // Add mapping between resource id and resource topology node.
  InsertIfNotPresent(&machine_to_rtnd_,
                     ResourceIDFromString(rtnd_ptr->resource_desc().uuid()),
                     rtnd_ptr);
}

void VoidCostModel::AddTask(TaskID_t task_id) {
}

void VoidCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machine_to_rtnd_.erase(res_id), 1);
}

void VoidCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* VoidCostModel::GatherStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK ||
      accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    return accumulator;
  }

  CHECK(accumulator->type_ == FlowNodeType::COORDINATOR ||
        accumulator->type_ == FlowNodeType::MACHINE ||
        accumulator->type_ == FlowNodeType::NUMA_NODE ||
        accumulator->type_ == FlowNodeType::SOCKET ||
        accumulator->type_ == FlowNodeType::CACHE ||
        accumulator->type_ == FlowNodeType::CORE ||
        accumulator->type_ == FlowNodeType::PU);

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

void VoidCostModel::PrepareStats(FlowGraphNode* accumulator) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK ||
      accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    // The node is not a resource.
    return;
  }
  CHECK(accumulator->type_ == FlowNodeType::COORDINATOR ||
        accumulator->type_ == FlowNodeType::MACHINE ||
        accumulator->type_ == FlowNodeType::NUMA_NODE ||
        accumulator->type_ == FlowNodeType::SOCKET ||
        accumulator->type_ == FlowNodeType::CACHE ||
        accumulator->type_ == FlowNodeType::CORE ||
        accumulator->type_ == FlowNodeType::PU);
  CHECK_NOTNULL(accumulator->rd_ptr_);
  accumulator->rd_ptr_->clear_num_running_tasks_below();
  accumulator->rd_ptr_->clear_num_slots_below();
}

FlowGraphNode* VoidCostModel::UpdateStats(FlowGraphNode* accumulator,
                                          FlowGraphNode* other) {
  return accumulator;
}

} // namespace firmament
