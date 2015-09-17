// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/octopus_cost_model.h"

#include <utility>
#include <vector>

#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/flow_graph_bridge.h"

DECLARE_bool(preemption);

namespace firmament {

OctopusCostModel::OctopusCostModel(shared_ptr<ResourceMap_t> resource_map,
                                   shared_ptr<TaskMap_t> task_map)
  : resource_map_(resource_map),
    task_map_(task_map) {
  // Create the cluster aggregator EC, which all machines are members of.
  cluster_aggregator_ec_ = HashString("CLUSTER_AGG");
  VLOG(1) << "Cluster aggregator EC is " << cluster_aggregator_ec_;
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
  // The cost in the Octopus model is the number of already running tasks, i.e.
  // a crude per-task load balancing algorithm.
  uint64_t num_tasks = dst_rd_ptr->num_running_tasks_below();
  return num_tasks;
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
                                                    EquivClass_t ec) {
  return 0ULL;
}

pair<Cost_t, int64_t> OctopusCostModel::EquivClassToResourceNode(
    EquivClass_t ec,
    ResourceID_t res_id) {
  return pair<Cost_t, int64_t>(0LL, -1LL);
}

Cost_t OctopusCostModel::EquivClassToEquivClass(EquivClass_t ec1,
                                                EquivClass_t ec2) {
  return 0LL;
}

vector<EquivClass_t>* OctopusCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // All tasks have an arc to the cluster aggregator, i.e. they are
  // all in the cluster aggregator EC.
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<EquivClass_t>* OctopusCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  // Only the cluster aggregator for the Octopus cost model
  equiv_classes->push_back(cluster_aggregator_ec_);
  return equiv_classes;
}

vector<ResourceID_t>* OctopusCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<ResourceID_t>* arc_destinations = new vector<ResourceID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator, and has arcs to all machines.
    // XXX(malte): This is inefficient, as it needlessly adds all the
    // machines every time we call this. To optimize, we can just include
    // the ones for which arcs are missing.
    for (auto it = machines_.begin();
         it != machines_.end();
         ++it) {
      arc_destinations->push_back(*it);
    }
  }
  return arc_destinations;
}

vector<TaskID_t>* OctopusCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t ec) {
  vector<TaskID_t>* tasks_with_incoming_arcs = new vector<TaskID_t>();
  if (ec == cluster_aggregator_ec_) {
    // ec is the cluster aggregator.
    // We add an arc from each task to the cluster aggregator.
    // XXX(malte): This is very slow because it iterates over all tasks; we
    // should instead only return the set of tasks that do not yet have the
    // appropriate arcs.
    for (TaskMap_t::iterator it = task_map_->begin(); it != task_map_->end();
         ++it) {
      // XXX(malte): task_map_ contains ALL tasks ever seen by the system,
      // including those that have completed, failed or are otherwise no longer
      // present in the flow graph. We do some crude filtering here, but clearly
      // we should instead maintain a collection of tasks actually eligible for
      // scheduling.
      if (it->second->state() == TaskDescriptor::RUNNABLE ||
          (FLAGS_preemption &&
           it->second->state() == TaskDescriptor::RUNNING)) {
        tasks_with_incoming_arcs->push_back(it->first);
      }
    }
  }
  return tasks_with_incoming_arcs;
}

vector<ResourceID_t>* OctopusCostModel::GetTaskPreferenceArcs(
    TaskID_t task_id) {
  // Not used in Octopus cost model
  return NULL;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    OctopusCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t ec) {
  return pair<vector<EquivClass_t>*,
              vector<EquivClass_t>*>(NULL, NULL);
}

void OctopusCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  // Keep track of the new machine
  CHECK(rtnd_ptr->resource_desc().type() ==
      ResourceDescriptor::RESOURCE_MACHINE);
  machines_.insert(ResourceIDFromString(rtnd_ptr->resource_desc().uuid()));
}

void OctopusCostModel::AddTask(TaskID_t task_id) {
}

void OctopusCostModel::RemoveMachine(ResourceID_t res_id) {
  CHECK_EQ(machines_.erase(res_id), 1);
}

void OctopusCostModel::RemoveTask(TaskID_t task_id) {
}

FlowGraphNode* OctopusCostModel::GatherStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK ||
      accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    // Node is neither part of the topology or an equivalence class.
    // We don't have to accumulate any state.
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    if (accumulator->type_ == FlowNodeType::PU) {
      // Base case. We are at a PU and we gather the statistics.
      ResourceStatus* rs_ptr =
        FindPtrOrNull(*resource_map_, accumulator->resource_id_);
      CHECK_NOTNULL(rs_ptr);
      ResourceDescriptor* rd_ptr = rs_ptr->mutable_descriptor();
      if (rd_ptr->has_current_running_task()) {
        rd_ptr->set_num_running_tasks_below(1);
      } else {
        rd_ptr->set_num_running_tasks_below(0);
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
  acc_rd_ptr->set_num_running_tasks_below(
      acc_rd_ptr->num_running_tasks_below() +
      other_rd_ptr->num_running_tasks_below());
  return accumulator;
}

FlowGraphNode* OctopusCostModel::UpdateStats(FlowGraphNode* accumulator,
                                             FlowGraphNode* other) {
  if (accumulator->type_ == FlowNodeType::ROOT_TASK ||
      accumulator->type_ == FlowNodeType::SCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::UNSCHEDULED_TASK ||
      accumulator->type_ == FlowNodeType::JOB_AGGREGATOR ||
      accumulator->type_ == FlowNodeType::SINK ||
      accumulator->type_ == FlowNodeType::EQUIVALENCE_CLASS) {
    return accumulator;
  }
  if (other->resource_id_.is_nil()) {
    return accumulator;
  }

  FlowGraphArc* arc = FlowGraph::GetArc(accumulator, other);
  uint64_t new_cost = ResourceNodeToResourceNodeCost(accumulator->resource_id_,
                                                     other->resource_id_);
  if (arc->cost_ != new_cost) {
    arc->cost_ = new_cost;
    DIMACSChange *chg = new DIMACSChangeArc(*arc);
    chg->set_comment("Octopus/UpdateStats");
    flow_graph_bridge_->AddGraphChange(chg);
  }

  // Reset the state.
  ResourceStatus* other_rs_ptr =
    FindPtrOrNull(*resource_map_, other->resource_id_);
  CHECK_NOTNULL(other_rs_ptr);
  ResourceDescriptor* other_rd_ptr = other_rs_ptr->mutable_descriptor();
  other_rd_ptr->set_num_running_tasks_below(0);
  return accumulator;
}

}  // namespace firmament
