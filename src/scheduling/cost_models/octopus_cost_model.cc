// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <utility>
#include <vector>

#include "misc/map-util.h"
#include "scheduling/cost_models/octopus_cost_model.h"
#include "scheduling/dimacs_change_arc.h"
#include "scheduling/flow_graph.h"

namespace firmament {

OctopusCostModel::OctopusCostModel(shared_ptr<ResourceMap_t> resource_map)
  : resource_map_(resource_map) {
}

Cost_t OctopusCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
  return 1000000LL;
}

Cost_t OctopusCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0LL;
}

Cost_t OctopusCostModel::TaskToClusterAggCost(TaskID_t task_id) {
  return 0LL;
}

Cost_t OctopusCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                                ResourceID_t resource_id) {
  return 0LL;
}

Cost_t OctopusCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t src, ResourceID_t dst) {
  ResourceStatus* dst_rs_ptr = FindPtrOrNull(*resource_map_, dst);
  CHECK_NOTNULL(dst_rs_ptr);
  ResourceDescriptor* dst_rd_ptr = dst_rs_ptr->mutable_descriptor();

  return dst_rd_ptr->num_running_tasks();
}

Cost_t OctopusCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0LL;
}

Cost_t OctopusCostModel::TaskContinuationCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t OctopusCostModel::TaskPreemptionCost(TaskID_t task_id) {
  return 0ULL;
}

Cost_t OctopusCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                    EquivClass_t tec) {
  return 0ULL;
}

Cost_t OctopusCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                                  ResourceID_t res_id) {
  return 0ULL;
}

Cost_t OctopusCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                                EquivClass_t tec2) {
  return 0LL;
}

vector<EquivClass_t>* OctopusCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  // CHECK_NOTNULL(td_ptr);
  // // A level 0 TEC is the hash of the task binary name.
  // size_t hash = 0;
  // boost::hash_combine(hash, td_ptr->binary());
  // equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* OctopusCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return equiv_classes;
}

vector<ResourceID_t>* OctopusCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

vector<TaskID_t>* OctopusCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<TaskID_t>* prefered_tasks = new vector<TaskID_t>();
  return prefered_tasks;
}

vector<ResourceID_t>* OctopusCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    OctopusCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(equiv_classes, equiv_classes);
}

void OctopusCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
}

void OctopusCostModel::RemoveMachine(ResourceID_t res_id) {
}

void OctopusCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* OctopusCostModel::GatherStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK ||
      accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    if (accumulator->type_.type() == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      CHECK_NOTNULL(rs_ptr);
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (rd_ptr->has_current_running_task()) {
        rd_ptr->set_num_running_tasks(1);
      } else {
        rd_ptr->set_num_running_tasks(0);
      }
    }
    return accumulator;
  }

  ResourceStatus* acc_rs_ptr =
    FindPtrOrNull(*resource_map_, accumulator->resource_id_);
  CHECK_NOTNULL(acc_rs_ptr);
  ResourceDescriptor* acc_rd_ptr = acc_rs_ptr->mutable_descriptor();

  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
  acc_rd_ptr->set_num_running_tasks(acc_rd_ptr->num_running_tasks() +
                                    other_rd_ptr->num_running_tasks());
  return accumulator;
}

FlowGraphNode* OctopusCostModel::UpdateStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  if (accumulator->type_.type() == FlowNodeType::ROOT_TASK ||
      accumulator->type_.type() == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_.type() == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_.type() == FlowNodeType::SINK ||
      accumulator->type_.type() == FlowNodeType::EQUIVALENCE_CLASS) {
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    return accumulator;
  }
  FlowGraphArc* arc = FlowGraph::GetArc(accumulator, other);
  arc->cost_ = ResourceNodeToResourceNodeCost(accumulator->resource_id_,
                                              other->resource_id_);
  flow_graph_->AddGraphChange(new DIMACSChangeArc(*arc));
  // Reset the state.
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
  other_rd_ptr->set_num_running_tasks(0);

  return accumulator;
}

}  // namespace firmament
