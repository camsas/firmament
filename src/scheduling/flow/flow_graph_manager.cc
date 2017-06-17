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

#include "scheduling/flow/flow_graph_manager.h"

#include <algorithm>
#include <limits>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include <cstdio>
#include <cstdlib>
#include <boost/bind.hpp>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/cost_model_utils.h"
#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"

DEFINE_bool(preemption, false, "Enable preemption and migration of tasks");
DEFINE_bool(update_preferences_running_task, false,
            "True if the preferences of a running task should be updated before"
            " each scheduling round");

DECLARE_string(flow_scheduling_solver);
DECLARE_uint64(max_tasks_per_pu);

namespace firmament {

FlowGraphManager::FlowGraphManager(
    CostModelInterface *cost_model,
    unordered_set<ResourceID_t,
    boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    TimeInterface* time_manager,
    TraceGenerator* trace_generator,
    DIMACSChangeStats* dimacs_stats)
    : cost_model_(cost_model),
      graph_change_manager_(new FlowGraphChangeManager(dimacs_stats)),
      leaf_res_ids_(leaf_res_ids),
      trace_generator_(trace_generator),
      dimacs_stats_(dimacs_stats),
      cur_traversal_counter_(0) {
  // Add sink node.
  sink_node_ = graph_change_manager_->AddNode(
      FlowNodeType::SINK, 0, ADD_SINK_NODE, "SINK");
  // N.B.: we do NOT create a cluster aggregator node here, since
  // not all cost models use one. Instead, cost models add it as a special
  // equivalence class.
}

FlowGraphManager::~FlowGraphManager() {
  // We don't delete cost_model_, leaf_res_ids_, trace_generator_ and
  // dimacs_stats_ because they are owned by the FlowScheduler.
  delete graph_change_manager_;
}

FlowGraphNode* FlowGraphManager::AddEquivClassNode(EquivClass_t ec) {
  FlowGraphNode* ec_node =
    graph_change_manager_->AddNode(FlowNodeType::EQUIVALENCE_CLASS,
                                   0, ADD_EQUIV_CLASS_NODE,
                                   "AddEquivClassNode");
  ec_node->ec_id_ = ec;
  CHECK(InsertIfNotPresent(&tec_to_node_map_, ec, ec_node));
  return ec_node;
}

void FlowGraphManager::AddOrUpdateJobNodes(
    const vector<JobDescriptor*>& jd_ptr_vect) {
  // For each job:
  // 1) Add/Update unscheduled agg node
  // 2) Add its root task to the queue
  queue<TDOrNodeWrapper*> node_queue;
  unordered_set<uint64_t> marked_nodes;
  for (const auto& jd_ptr : jd_ptr_vect) {
    JobID_t job_id = JobIDFromString(jd_ptr->uuid());
    // First add an unscheduled aggregator node for this job
    // if none exists already.
    FlowGraphNode* unsched_agg_node = UnschedAggNodeForJobID(job_id);
    if (!unsched_agg_node) {
      unsched_agg_node = AddUnscheduledAggNode(job_id);
    }
    TaskDescriptor* root_td_ptr = jd_ptr->mutable_root_task();
    FlowGraphNode* root_task_node = NodeForTaskID(root_td_ptr->uid());
    if (!root_task_node) {
      if (TaskMustHaveNode(*root_td_ptr)) {
        root_task_node = AddTaskNode(job_id, root_td_ptr);
        // Increment capacity from unsched agg node to sink.
        UpdateUnscheduledAggNode(unsched_agg_node, 1);
        node_queue.push(new TDOrNodeWrapper(root_task_node, root_td_ptr));
        marked_nodes.insert(root_task_node->id_);
      } else {
        // We don't have to add a new node for the task.
        node_queue.push(new TDOrNodeWrapper(root_td_ptr));
        // NOTE: We can't mark the task as visited because we don't have
        // a node id for it. However, this is fine in practice because the
        // tasks cannot be a DAG and so we will never visit them again.
      }
    } else {
      node_queue.push(new TDOrNodeWrapper(root_task_node, root_td_ptr));
      marked_nodes.insert(root_task_node->id_);
    }
  }
  // UpdateFlowGraph is responsible for making sure that the node_queue is
  // empty upon completion.
  UpdateFlowGraph(&node_queue, &marked_nodes);
}

void FlowGraphManager::AddResourceTopologyDFS(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  // Steps:
  // 1) Add new resource node and connect it to the sink if the new node is a
  // PU node.
  // 2) Add the node's subtree.
  // 3) Connect the node to its parent.
  bool added_new_res_node = false;
  CHECK_NOTNULL(rtnd_ptr);
  ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
  CHECK_NOTNULL(rd_ptr);
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  FlowGraphNode* res_node = NodeForResourceID(res_id);
  if (!res_node) {
    added_new_res_node = true;
    res_node = AddResourceNode(rd_ptr);
    if (res_node->type_ == FlowNodeType::PU) {
      UpdateResToSinkArc(res_node);
      if (rd_ptr->num_slots_below() == 0) {
        rd_ptr->set_num_slots_below(FLAGS_max_tasks_per_pu);
        if (rd_ptr->num_running_tasks_below() == 0) {
          rd_ptr->set_num_running_tasks_below(
              static_cast<uint64_t>(rd_ptr->current_running_tasks_size()));
        }
      }
    } else {
      if (res_node->type_ == FlowNodeType::MACHINE) {
        trace_generator_->AddMachine(*rd_ptr);
        cost_model_->AddMachine(rtnd_ptr);
      }
      rd_ptr->set_num_slots_below(0);
      rd_ptr->set_num_running_tasks_below(0);
    }
  } else {
    rd_ptr->set_num_slots_below(0);
    rd_ptr->set_num_running_tasks_below(0);
    // TODO(ionel): The method continues even if we already had a node for the
    // "new" resources. This is because the coordinator ends up calling twice
    // RegisterResource for the same resource. Uncomment the LOG(FATAL) once
    // the coordinator is fixed.
    // (see https://github.com/ms705/firmament/issues/41)
    // LOG(FATAL) << "Resource node for resource: " << res_id
    //            << " already exists";
  }
  VisitTopologyChildren(rtnd_ptr);
  if (rtnd_ptr->parent_id().empty()) {
    CHECK_EQ(rtnd_ptr->resource_desc().type(),
             ResourceDescriptor::RESOURCE_COORDINATOR)
      << "A resource node that is not a coordinator must have a parent";
  } else if (added_new_res_node) {
    // Connect the node to the parent.
    FlowGraphNode* parent_node =
      NodeForResourceID(ResourceIDFromString(rtnd_ptr->parent_id()));
    CHECK_NOTNULL(parent_node);
    CHECK(InsertIfNotPresent(&node_to_parent_node_map_, res_node, parent_node));
    ArcDescriptor arc_descriptor =
      cost_model_->ResourceNodeToResourceNode(*parent_node->rd_ptr_, *rd_ptr);
    graph_change_manager_->AddArc(
        parent_node, res_node, arc_descriptor.min_flow_,
        arc_descriptor.capacity_, arc_descriptor.cost_,
        OTHER, ADD_ARC_BETWEEN_RES, "AddResourceTopologyDFS");
  }
}

void FlowGraphManager::AddResourceTopology(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  AddResourceTopologyDFS(rtnd_ptr);
  // Progapate the capacity increase to the root of the topology.
  if (!rtnd_ptr->parent_id().empty()) {
    // We start from rtnd_ptr's parent because in AddResourceTopologyDFS we
    // already added an arc between rtnd_ptr and its parent.
    FlowGraphNode* cur_node =
      NodeForResourceID(ResourceIDFromString(rtnd_ptr->parent_id()));
    int64_t running_tasks_delta =
      static_cast<int64_t>(rtnd_ptr->resource_desc().num_running_tasks_below());
    int64_t capacity_to_parent = static_cast<int64_t>(
        CapacityFromResNodeToParent(rtnd_ptr->resource_desc()));
    UpdateResourceStatsUpToRoot(
        cur_node, capacity_to_parent,
        static_cast<int64_t>(rtnd_ptr->resource_desc().num_slots_below()),
        running_tasks_delta);
  }
}

FlowGraphNode* FlowGraphManager::AddResourceNode(ResourceDescriptor* rd_ptr) {
  CHECK_NOTNULL(rd_ptr);
  string comment;
  if (!rd_ptr->friendly_name().empty()) {
    comment = rd_ptr->friendly_name();
  } else {
    comment = "AddResourceNode";
  }
  FlowGraphNode* res_node = graph_change_manager_->AddNode(
      FlowGraphNode::TransformToResourceNodeType(*rd_ptr),
      0, ADD_RESOURCE_NODE, comment.c_str());
  ResourceID_t res_id = ResourceIDFromString(rd_ptr->uuid());
  res_node->resource_id_ = res_id;
  res_node->rd_ptr_ = rd_ptr;
  CHECK(InsertIfNotPresent(&resource_to_node_map_, res_id, res_node));
  if (res_node->type_ == FlowNodeType::PU) {
    leaf_nodes_.insert(res_node->id_);
    leaf_res_ids_->insert(res_id);
  }
  return res_node;
}

FlowGraphNode* FlowGraphManager::AddTaskNode(JobID_t job_id,
                                             TaskDescriptor* td_ptr) {
  CHECK_NOTNULL(td_ptr);
  // TODO(ionel): move to place where the method is called form.
  trace_generator_->TaskSubmitted(td_ptr);
  cost_model_->AddTask(td_ptr->uid());
  FlowGraphNode* task_node =
    graph_change_manager_->AddNode(FlowNodeType::UNSCHEDULED_TASK, 1,
                                   ADD_TASK_NODE, "AddTaskNode");
  task_node->td_ptr_ = td_ptr;
  task_node->job_id_ = job_id;
  sink_node_->excess_--;
  CHECK(InsertIfNotPresent(&task_to_node_map_, td_ptr->uid(), task_node));
  return task_node;
}

FlowGraphNode* FlowGraphManager::AddUnscheduledAggNode(JobID_t job_id) {
  string comment = "UNSCHED_AGG_for_" + to_string(job_id);
  FlowGraphNode* unsched_agg_node = graph_change_manager_->AddNode(
      FlowNodeType::JOB_AGGREGATOR, 0, ADD_UNSCHED_JOB_NODE, comment.c_str());
  CHECK(InsertIfNotPresent(&job_unsched_to_node_, job_id, unsched_agg_node));
  return unsched_agg_node;
}

void FlowGraphManager::ComputeTopologyStatistics(
    FlowGraphNode* node,
    boost::function<void(FlowGraphNode*)> prepare,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> gather,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> update) {
  // XXX(ionel): The function only works correctly as long as the topology is a
  // tree. If the topology is a DAG then it does not work correctly! It does
  // not work in the DAG case because the function implements BFS. Hence,
  // we may pop a not of the queue and propagate its statistics via its incoming
  // arcs before we've received all the statistics at the node.
  queue<FlowGraphNode*> to_visit;
  // We maintain a value that is used to mark visited nodes. Before each
  // visit we increment the mark to make sure that nodes visited in previous
  // traversal are not going to be treated as marked. By using the mark
  // variable we avoid having to reset the visited state of each node before
  // of a traversal.
  cur_traversal_counter_++;
  to_visit.push(node);
  node->visited_ = cur_traversal_counter_;
  while (!to_visit.empty()) {
    FlowGraphNode* cur_node = to_visit.front();
    to_visit.pop();
    for (auto& incoming_arc : cur_node->incoming_arc_map_) {
      if (incoming_arc.second->src_node_->visited_ != cur_traversal_counter_) {
        if (prepare) {
          prepare(incoming_arc.second->src_node_);
        }
        to_visit.push(incoming_arc.second->src_node_);
        incoming_arc.second->src_node_->visited_ = cur_traversal_counter_;
      }
      incoming_arc.second->src_node_ =
        gather(incoming_arc.second->src_node_, cur_node);
      incoming_arc.second->src_node_ =
        update(incoming_arc.second->src_node_, cur_node);
    }
  }
}

void FlowGraphManager::JobCompleted(JobID_t job_id) {
  RemoveUnscheduledAggNode(job_id);
  // We don't have to do anything else here. The task nodes have already been
  // removed.
}

void FlowGraphManager::JobRemoved(JobID_t job_id) {
  RemoveUnscheduledAggNode(job_id);
  // We don't have to do anything else here. The task nodes have already been
  // removed.
}

void FlowGraphManager::SchedulingDeltasForPreemptedTasks(
    const multimap<uint64_t, uint64_t>& task_mappings,
    shared_ptr<ResourceMap_t> resource_map,
    vector<SchedulingDelta*>* deltas) {
  for (auto& res_id_status : *resource_map) {
    ResourceDescriptor* rd_ptr = res_id_status.second->mutable_descriptor();
    RepeatedField<uint64_t> running_tasks = rd_ptr->current_running_tasks();
    for (auto& task_id : running_tasks) {
      FlowGraphNode* task_node = NodeForTaskID(task_id);
      if (!task_node) {
        // There's no node for the task => we don't need to generate
        // a PREEMPT delta because the task has finished.
        continue;
      }
      const uint64_t* res_node_id = FindOrNull(task_mappings, task_node->id_);
      if (!res_node_id) {
        // The task doesn't exist in the mappings => the task has been
        // preempted.
        VLOG(2) << "PREEMPTION: take " << task_id << " off "
                << res_id_status.first;
        SchedulingDelta* preempt_delta = new SchedulingDelta;
        preempt_delta->set_type(SchedulingDelta::PREEMPT);
        preempt_delta->set_task_id(task_id);
        preempt_delta->set_resource_id(rd_ptr->uuid());
        deltas->push_back(preempt_delta);
      }
    }
    // We clear all the running tasks on the machine. The list is going to be
    // populated again in NodeBindingToSchedulingDeltas and
    // EventDrivenScheduler.
    // It is easier and less expensive to clear it and populate it back again
    // than making sure the preempted tasks are removed.
    rd_ptr->clear_current_running_tasks();
  }
}

void FlowGraphManager::NodeBindingToSchedulingDeltas(
    uint64_t task_node_id, uint64_t res_node_id,
    unordered_map<TaskID_t, ResourceID_t>* task_bindings,
    vector<SchedulingDelta*>* deltas) {
  const FlowGraphNode& task_node = graph_change_manager_->Node(task_node_id);
  CHECK(task_node.IsTaskNode());
  // Destination must be a PU node
  const FlowGraphNode& res_node = graph_change_manager_->Node(res_node_id);
  CHECK(res_node.type_ == FlowNodeType::PU);
  CHECK_NOTNULL(task_node.td_ptr_);
  const TaskDescriptor& task = *task_node.td_ptr_;
  CHECK_NOTNULL(res_node.rd_ptr_);
  const ResourceDescriptor& res = *res_node.rd_ptr_;
  // Is the source (task) already placed elsewhere?
  ResourceID_t* bound_res = FindOrNull(*task_bindings, task.uid());
  if (bound_res) {
    // Task already running somewhere.
    if (*bound_res != ResourceIDFromString(res.uuid())) {
      // Task is running on a different resource => migration.
      VLOG(2) << "MIGRATION: take " << task.uid() << " off "
              << *bound_res << " and move it to "
              << res.uuid();
      SchedulingDelta* delta = new SchedulingDelta;
      delta->set_type(SchedulingDelta::MIGRATE);
      delta->set_task_id(task.uid());
      delta->set_resource_id(res.uuid());
      deltas->push_back(delta);
    } else {
      // We were already scheduled here. Add back the task_id to the resource's
      // running tasks list.
      res_node.rd_ptr_->add_current_running_tasks(task.uid());
    }
  } else {
    // Place the task.
    VLOG(2) << "SCHEDULING: place " << task.uid() << " on " << res.uuid();
    SchedulingDelta* place_delta = new SchedulingDelta;
    place_delta->set_type(SchedulingDelta::PLACE);
    place_delta->set_task_id(task.uid());
    place_delta->set_resource_id(res.uuid());
    deltas->push_back(place_delta);
  }
}

void FlowGraphManager::PinTaskToNode(FlowGraphNode* task_node,
                                     FlowGraphNode* res_node) {
  bool added_running_arc = false;
  // Remove all arcs apart from the task -> resource mapping.
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         task_node->outgoing_arc_map_.begin();
       it != task_node->outgoing_arc_map_.end(); ) {
    FlowGraphArc* arc = it->second;
    ++it;
    if (arc->dst_node_->id_ == res_node->id_) {
      // This preference arc connects the same nodes as the running arc. Hence,
      // we just transform it into the running arc.
      added_running_arc = true;
      ArcDescriptor arc_descriptor =
        cost_model_->TaskContinuation(task_node->td_ptr_->uid());
      arc->type_ = RUNNING;
      // TODO(ionel): Task pinning should be handled from cost models. They
      // should return min_flow_ = 1
      uint64_t low_bound_capacity = 1;
      if (FLAGS_flow_scheduling_solver == "custom") {
        // XXX(ionel): RelaxIV doesn't print the flow on the arcs on which
        // the lower capacity bound is equal to the upper capacity bound.
        // We work around this by simply not setting the lower bound capacity
        // for custom solvers.
        low_bound_capacity = 0;
      }
      graph_change_manager_->ChangeArc(
          arc, low_bound_capacity, 1, arc_descriptor.cost_,
          CHG_ARC_RUNNING_TASK, "PinTaskToNode: transform to running arc");
      CHECK(InsertIfNotPresent(&task_to_running_arc_,
                               task_node->td_ptr_->uid(), arc));
    } else {
      // TODO(ionel): This doesn't correctly compute the type of changes. The
      // arcs we are deleting can point to unscheduled or equiv classes as well.
      graph_change_manager_->DeleteArc(arc, DEL_ARC_TASK_TO_EQUIV_CLASS,
                                       "PinTaskNode");
    }
  }
  // Decrement capacity from unsched agg node to sink.
  UpdateUnscheduledAggNode(UnschedAggNodeForJobID(task_node->job_id_), -1);
  if (!added_running_arc) {
    // TODO(ionel): Task pinning should be handled from cost models. They
    // should return min_flow_ = 1
    ArcDescriptor arc_descriptor =
      cost_model_->TaskContinuation(task_node->td_ptr_->uid());
    uint64_t low_bound_capacity = 1;
    if (FLAGS_flow_scheduling_solver == "custom") {
      // XXX(ionel): RelaxIV doesn't print the flow on the arcs on which
      // the lower capacity bound is equal to the upper capacity bound.
      // We work around this by simply not setting the lower bound capacity
      // for custom solvers.
      low_bound_capacity = 0;
    }
    // Add a single arc from the task to the resource node
    FlowGraphArc* new_arc = graph_change_manager_->AddArc(
        task_node, res_node, low_bound_capacity, 1, arc_descriptor.cost_,
        RUNNING, ADD_ARC_RUNNING_TASK, "PinTaskToNode: add running arc");
    CHECK(InsertIfNotPresent(&task_to_running_arc_,
                             task_node->td_ptr_->uid(), new_arc));
  }
}

void FlowGraphManager::PurgeUnconnectedEquivClassNodes() {
  // NOTE: we could have a subgraph consisting of equiv class nodes.
  // They would likely not end up being removed in a single
  // PurgeUnconnectedEquivClassNodes call. However, this is fine
  // because we will finish removing all of them in future calls.
  for (unordered_map<EquivClass_t, FlowGraphNode*>::iterator
         it = tec_to_node_map_.begin();
       it != tec_to_node_map_.end(); ) {
    FlowGraphNode* ec_node = it->second;
    ++it;
    if (ec_node->incoming_arc_map_.size() == 0) {
      RemoveEquivClassNode(ec_node);
    }
  }
}

void FlowGraphManager::RemoveEquivClassNode(FlowGraphNode* ec_node) {
  CHECK_NOTNULL(ec_node);
  tec_to_node_map_.erase(ec_node->ec_id_);
  graph_change_manager_->DeleteNode(ec_node, DEL_EQUIV_CLASS_NODE,
                                    "RemoveEquivClassNode");
}

void FlowGraphManager::RemoveInvalidECPrefArcs(
    const FlowGraphNode& node,
    const vector<EquivClass_t>& pref_ecs,
    DIMACSChangeType change_type) {
  unordered_set<EquivClass_t> ec_preferences(pref_ecs.begin(), pref_ecs.end());
  unordered_set<FlowGraphArc*> to_delete;
  for (auto& dst_arc : node.outgoing_arc_map_) {
    EquivClass_t pref_ec = dst_arc.second->dst_node_->ec_id_;
    // Remove if the pref is an EC node and it's not in the preferences vector
    if (pref_ec != 0 && ec_preferences.find(pref_ec) == ec_preferences.end()) {
      to_delete.insert(dst_arc.second);
      VLOG(2) << "Deleting no-longer-current arc to EC " << pref_ec;
    }
  }
  for (auto& arc : to_delete) {
    graph_change_manager_->DeleteArc(arc, change_type,
                                     "RemoveInvalidECPrefArcs");
  }
}

void FlowGraphManager::RemoveInvalidPrefResArcs(
    const FlowGraphNode& node,
    const vector<ResourceID_t>& pref_resources,
    DIMACSChangeType change_type) {
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>> res_preferences(
      pref_resources.begin(),
      pref_resources.end());
  unordered_set<FlowGraphArc*> to_delete;
  for (auto& dst_arc : node.outgoing_arc_map_) {
    ResourceID_t pref_rid = dst_arc.second->dst_node_->resource_id_;
    // Remove if the pref node is a resource node and it's not in the
    // preferences set.
    if (!pref_rid.is_nil() &&
        res_preferences.find(pref_rid) == res_preferences.end() &&
        dst_arc.second->type_ != FlowGraphArcType::RUNNING) {
      to_delete.insert(dst_arc.second);
      VLOG(2) << "Deleting no-longer-current arc to resource " << pref_rid;
    }
  }
  for (auto& arc : to_delete) {
    graph_change_manager_->DeleteArc(arc, change_type,
                                     "RemoveInvalidResPrefArcs");
  }
}

void FlowGraphManager::RemoveResourceTopology(const ResourceDescriptor& rd,
                                              set<uint64_t>* pus_removed) {
  CHECK_NOTNULL(pus_removed);
  ResourceID_t res_id = ResourceIDFromString(rd.uuid());
  FlowGraphNode* res_node = NodeForResourceID(res_id);
  CHECK_NOTNULL(res_node);
  int64_t cap_delta = 0;
  // Delete the children nodes. We use an iterator because we change the
  // collection while we iterate over it.
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator
         it = res_node->outgoing_arc_map_.begin();
       it != res_node->outgoing_arc_map_.end();) {
    FlowGraphArc* arc = it->second;
    ++it;
    cap_delta -=  arc->cap_upper_bound_;
    if (!arc->dst_node_->resource_id_.is_nil()) {
      TraverseAndRemoveTopology(arc->dst_node_, pus_removed);
    }
  }
  // Propagate the stats update up to the root resource.
  UpdateResourceStatsUpToRoot(
      res_node, cap_delta,
      -(static_cast<int64_t>(res_node->rd_ptr_->num_slots_below())),
      -(static_cast<int64_t>(res_node->rd_ptr_->num_running_tasks_below())));
  // Delete the node.
  if (res_node->type_ == FlowNodeType::PU) {
    pus_removed->insert(res_node->id_);
  } else if (res_node->type_ == FlowNodeType::MACHINE) {
    cost_model_->RemoveMachine(res_node->resource_id_);
  }
  RemoveResourceNode(res_node);
}

void FlowGraphManager::RemoveResourceNode(FlowGraphNode* res_node) {
  CHECK_NOTNULL(res_node);
  if (node_to_parent_node_map_.erase(res_node) == 0) {
    LOG(WARNING) << "Removing root resource node!";
  }
  // No need to check erase result, as the call may not delete anything if the
  // resource is not a leaf.
  leaf_nodes_.erase(res_node->id_);
  // When we call erase() on a set we end up deleting the object because the set
  // calls the object's destructor. We copy res_id to avoid using freed memory.
  ResourceID_t res_id_tmp = res_node->resource_id_;
  ResourceID_t res_id_tmp2 = res_node->resource_id_;
  leaf_res_ids_->erase(res_id_tmp);
  resource_to_node_map_.erase(res_id_tmp2);
  graph_change_manager_->DeleteNode(res_node, DEL_RESOURCE_NODE,
                                    "RemoveResourceNode");
}

void FlowGraphManager::RemoveTaskHelper(TaskID_t task_id) {
  FlowGraphNode* task_node = NodeForTaskID(task_id);
  // task_node may be NULL if the task already completed.
  if (task_node) {
    if (FLAGS_preemption) {
      // We reduce the capacity from the unscheduled aggregator to the sink when
      // we pin the task. Hence, we only have to reduce the capacity when we
      // support preemption.
      UpdateUnscheduledAggNode(UnschedAggNodeForJobID(task_node->job_id_), -1);
    }
    task_to_running_arc_.erase(task_id);
    RemoveTaskNode(task_node);
    cost_model_->RemoveTask(task_id);
  }
}

uint64_t FlowGraphManager::RemoveTaskNode(FlowGraphNode* task_node) {
  CHECK_NOTNULL(task_node);
  uint64_t task_node_id = task_node->id_;
  // Increase the sink's excess and set this node's excess to zero.
  task_node->excess_ = 0;
  sink_node_->excess_++;
  CHECK_EQ(task_to_node_map_.erase(task_node->td_ptr_->uid()), 1);
  graph_change_manager_->DeleteNode(task_node, DEL_TASK_NODE, "RemoveTaskNode");
  return task_node_id;
}

void FlowGraphManager::RemoveUnscheduledAggNode(JobID_t job_id) {
  FlowGraphNode* unsched_agg_node = UnschedAggNodeForJobID(job_id);
  CHECK_NOTNULL(unsched_agg_node);
  CHECK_EQ(job_unsched_to_node_.erase(job_id), 1);
  graph_change_manager_->DeleteNode(unsched_agg_node, DEL_UNSCHED_JOB_NODE,
                                    "RemoveUnscheduledAggNode");
}

uint64_t FlowGraphManager::TaskCompleted(TaskID_t task_id) {
  FlowGraphNode* task_node = NodeForTaskID(task_id);
  CHECK_NOTNULL(task_node);
  if (FLAGS_preemption) {
    // When we pin the task we reduce the capacity from the unscheduled
    // aggrator to the sink. Hence, we only have to reduce the capacity
    // when we support preemption.
    UpdateUnscheduledAggNode(UnschedAggNodeForJobID(task_node->job_id_), -1);
  }
  task_to_running_arc_.erase(task_id);
  uint64_t task_node_id = RemoveTaskNode(task_node);
  // NOTE: We do not remove the task from the cost_model because
  // HandleTaskFinalReport still needs to get the task's  equivalence classes.
  return task_node_id;
}

void FlowGraphManager::TaskEvicted(TaskID_t task_id, ResourceID_t res_id) {
  FlowGraphNode* task_node = NodeForTaskID(task_id);
  CHECK_NOTNULL(task_node);
  task_node->type_ = FlowNodeType::UNSCHEDULED_TASK;
  FlowGraphArc* running_arc =
    FindPtrOrNull(task_to_running_arc_, task_node->td_ptr_->uid());
  CHECK_NOTNULL(running_arc);
  task_to_running_arc_.erase(task_id);
  graph_change_manager_->DeleteArc(running_arc, DEL_ARC_EVICTED_TASK,
                                   "TaskEvicted: delete running arc");
  if (!FLAGS_preemption) {
    // If we're running with preemption disabled then increase the capacity from
    // the unscheduled aggregator to the sink because the task can now stay
    // unscheduled.
    FlowGraphNode* unsched_agg_node =
      UnschedAggNodeForJobID(JobIDFromString(task_node->td_ptr_->job_id()));
    CHECK_NOTNULL(unsched_agg_node);
    // Increment capacity from unsched agg node to sink.
    UpdateUnscheduledAggNode(unsched_agg_node, 1);
  }
  // The task's arcs will be updated just before the next solver run.
}

void FlowGraphManager::TaskFailed(TaskID_t task_id) {
  RemoveTaskHelper(task_id);
}

void FlowGraphManager::TaskKilled(TaskID_t task_id) {
  RemoveTaskHelper(task_id);
}

void FlowGraphManager::TaskMigrated(TaskID_t task_id,
                                    ResourceID_t old_res_id,
                                    ResourceID_t new_res_id) {
  TaskEvicted(task_id, old_res_id);
  TaskScheduled(task_id, new_res_id);
}

void FlowGraphManager::TaskRemoved(TaskID_t task_id) {
  RemoveTaskHelper(task_id);
}

void FlowGraphManager::TaskScheduled(TaskID_t task_id, ResourceID_t res_id) {
  FlowGraphNode* task_node = NodeForTaskID(task_id);
  CHECK_NOTNULL(task_node);
  task_node->type_ = FlowNodeType::SCHEDULED_TASK;
  FlowGraphNode* res_node = NodeForResourceID(res_id);
  UpdateArcsForScheduledTask(task_node, res_node);
}

void FlowGraphManager::TraverseAndRemoveTopology(FlowGraphNode* res_node,
                                                 set<uint64_t>* pus_removed) {
  // We use an iterator because we change the collection while we iterate over
  // it.
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator
         it = res_node->outgoing_arc_map_.begin();
       it != res_node->outgoing_arc_map_.end();) {
    FlowGraphArc* arc = it->second;
    ++it;
    if (!arc->dst_node_->resource_id_.is_nil()) {
      // The arc is pointing to a resource node.
      TraverseAndRemoveTopology(arc->dst_node_, pus_removed);
    }
  }
  if (res_node->type_ == FlowNodeType::PU) {
    pus_removed->insert(res_node->id_);
  } else if (res_node->type_ == FlowNodeType::MACHINE) {
    cost_model_->RemoveMachine(res_node->resource_id_);
  }
  RemoveResourceNode(res_node);
}

void FlowGraphManager::UpdateAllCostsToUnscheduledAggs() {
  for (auto& job_node : job_unsched_to_node_) {
    const FlowGraphNode* unsched_node = job_node.second;
    CHECK_NOTNULL(unsched_node);
    for (auto& dst_arc : unsched_node->incoming_arc_map_) {
      FlowGraphNode* task_node = dst_arc.second->src_node_;
      CHECK_NOTNULL(task_node->td_ptr_);
      if (task_node->IsTaskAssignedOrRunning()) {
        UpdateRunningTaskNode(task_node, false, NULL, NULL);
      } else {
        UpdateTaskToUnscheduledAggArc(task_node);
      }
    }
  }
}

void FlowGraphManager::UpdateArcsForScheduledTask(FlowGraphNode* task_node,
                                                  FlowGraphNode* res_node) {
  CHECK_NOTNULL(task_node);
  CHECK_NOTNULL(res_node);
  if (FLAGS_preemption) {
    // We do not remove any old arcs. We only add/change a running arc to
    // the resource.
    ArcDescriptor arc_descriptor =
      cost_model_->TaskContinuation(task_node->td_ptr_->uid());
    FlowGraphArc* running_arc =
      FindPtrOrNull(task_to_running_arc_, task_node->td_ptr_->uid());
    if (running_arc) {
      // The running arc points to the same destination as a preference arc.
      // We just modify the preference arc because the graph doesn't currently
      // support multi-arcs.
      running_arc->type_ = RUNNING;
      graph_change_manager_->ChangeArc(
          running_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
          arc_descriptor.cost_, CHG_ARC_RUNNING_TASK,
          "UpdateArcsForScheduledTask: transform to running arc");
    } else {
      running_arc = graph_change_manager_->AddArc(
          task_node, res_node, arc_descriptor.min_flow_,
          arc_descriptor.capacity_, arc_descriptor.cost_, RUNNING,
          ADD_ARC_RUNNING_TASK, "UpdateArcsForScheduledTask: add running arc");
      CHECK(InsertIfNotPresent(&task_to_running_arc_, task_node->td_ptr_->uid(),
                               running_arc));
    }
    UpdateRunningTaskToUnscheduledAggArc(task_node);
  } else {
    PinTaskToNode(task_node, res_node);
  }
}

void FlowGraphManager::UpdateChildrenTasks(
    TaskDescriptor* td_ptr,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  // We do actually need to push tasks even if they are already completed,
  // failed or running, since they may have children eligible for
  // scheduling.
  CHECK_NOTNULL(td_ptr);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  for (RepeatedPtrField<TaskDescriptor>::pointer_iterator
         task_iter = td_ptr->mutable_spawned()->pointer_begin();
       task_iter != td_ptr->mutable_spawned()->pointer_end();
       ++task_iter) {
    TaskDescriptor* child_td_ptr = *task_iter;
    FlowGraphNode* child_task_node = NodeForTaskID(child_td_ptr->uid());
    if (!child_task_node) {
      if (TaskMustHaveNode(*child_td_ptr)) {
        JobID_t job_id = JobIDFromString(child_td_ptr->job_id());
        child_task_node = AddTaskNode(job_id, child_td_ptr);
        // Increment capacity from unsched agg node to sink.
        UpdateUnscheduledAggNode(UnschedAggNodeForJobID(job_id), 1);
        node_queue->push(new TDOrNodeWrapper(child_task_node, child_td_ptr));
        marked_nodes->insert(child_task_node->id_);
      } else {
        node_queue->push(new TDOrNodeWrapper(child_td_ptr));
      }
    } else {
      if (marked_nodes->find(child_task_node->id_) == marked_nodes->end()) {
        node_queue->push(new TDOrNodeWrapper(child_task_node, child_td_ptr));
        marked_nodes->insert(child_task_node->id_);
      }
    }
  }
}

void FlowGraphManager::UpdateEquivClassNode(
    FlowGraphNode* ec_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(ec_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  UpdateEquivToEquivArcs(ec_node, node_queue, marked_nodes);
  UpdateEquivToResArcs(ec_node, node_queue, marked_nodes);
}

void FlowGraphManager::UpdateEquivToEquivArcs(
    FlowGraphNode* ec_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(ec_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  vector<EquivClass_t>* pref_ec =
    cost_model_->GetEquivClassToEquivClassesArcs(ec_node->ec_id_);
  if (pref_ec) {
    for (auto& pref_ec_id : *pref_ec) {
      FlowGraphNode* pref_ec_node = NodeForEquivClass(pref_ec_id);
      if (!pref_ec_node) {
        pref_ec_node = AddEquivClassNode(pref_ec_id);
      }
      ArcDescriptor arc_descriptor =
        cost_model_->EquivClassToEquivClass(ec_node->ec_id_, pref_ec_id);
      FlowGraphArc* pref_ec_arc =
        graph_change_manager_->mutable_flow_graph()->GetArc(ec_node,
                                                            pref_ec_node);
      if (!pref_ec_arc) {
        graph_change_manager_->AddArc(
            ec_node, pref_ec_node, arc_descriptor.min_flow_,
            arc_descriptor.capacity_, arc_descriptor.cost_, OTHER,
            ADD_ARC_BETWEEN_EQUIV_CLASS, "UpdateEquivClassNode");
      } else {
        graph_change_manager_->ChangeArc(
            pref_ec_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
            arc_descriptor.cost_, CHG_ARC_BETWEEN_EQUIV_CLASS,
            "UpdateEquivClassNode");
      }
      if (marked_nodes->find(pref_ec_node->id_) == marked_nodes->end()) {
        // Add the EC node to the queue if it hasn't been marked yet.
        marked_nodes->insert(pref_ec_node->id_);
        node_queue->push(
            new TDOrNodeWrapper(pref_ec_node, pref_ec_node->td_ptr_));
      }
    }
    RemoveInvalidECPrefArcs(*ec_node, *pref_ec, DEL_ARC_BETWEEN_EQUIV_CLASS);
    delete pref_ec;
  } else {
    vector<EquivClass_t> no_pref_ec;
    RemoveInvalidECPrefArcs(*ec_node, no_pref_ec, DEL_ARC_BETWEEN_EQUIV_CLASS);
  }
}

void FlowGraphManager::UpdateEquivToResArcs(
    FlowGraphNode* ec_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(ec_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  vector<ResourceID_t>* pref_res =
    cost_model_->GetOutgoingEquivClassPrefArcs(ec_node->ec_id_);
  if (pref_res) {
    for (auto& pref_res_id : *pref_res) {
      FlowGraphNode* pref_res_node = NodeForResourceID(pref_res_id);
      // The resource node should already exist because the cost models cannot
      // prefer a resource before it is added to the graph.
      CHECK_NOTNULL(pref_res_node);
      ArcDescriptor arc_descriptor =
        cost_model_->EquivClassToResourceNode(ec_node->ec_id_, pref_res_id);
      FlowGraphArc* pref_res_arc =
        graph_change_manager_->mutable_flow_graph()->GetArc(ec_node,
                                                            pref_res_node);
      if (!pref_res_arc) {
        graph_change_manager_->AddArc(
            ec_node, pref_res_node, arc_descriptor.min_flow_,
            arc_descriptor.capacity_, arc_descriptor.cost_,
            OTHER, ADD_ARC_EQUIV_CLASS_TO_RES, "UpdateEquivToResArcs");

      } else {
        graph_change_manager_->ChangeArc(
            pref_res_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
            arc_descriptor.cost_, CHG_ARC_EQUIV_CLASS_TO_RES,
            "UpdateEquivToResArcs");
      }
      if (marked_nodes->find(pref_res_node->id_) == marked_nodes->end()) {
        // Add the res node to the queue if it hasn't been marked yet.
        marked_nodes->insert(pref_res_node->id_);
        node_queue->push(
            new TDOrNodeWrapper(pref_res_node, pref_res_node->td_ptr_));
      }
    }
    RemoveInvalidPrefResArcs(*ec_node, *pref_res, DEL_ARC_EQUIV_CLASS_TO_RES);
    delete pref_res;
  } else {
    vector<ResourceID_t> no_pref_res;
    RemoveInvalidPrefResArcs(*ec_node, no_pref_res, DEL_ARC_EQUIV_CLASS_TO_RES);
  }
}

void FlowGraphManager::UpdateFlowGraph(
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  while (!node_queue->empty()) {
    TDOrNodeWrapper* cur_node = node_queue->front();
    node_queue->pop();
    if (!cur_node->node_) {
      // We're handling a task that doesn't have an associated flow graph node.
      UpdateChildrenTasks(cur_node->td_ptr_, node_queue, marked_nodes);
      delete cur_node;
      continue;
    }
    if (cur_node->node_->IsTaskNode()) {
      UpdateTaskNode(cur_node->node_, node_queue, marked_nodes);
      UpdateChildrenTasks(cur_node->td_ptr_, node_queue, marked_nodes);
    } else if (cur_node->node_->IsEquivalenceClassNode()) {
      UpdateEquivClassNode(cur_node->node_, node_queue, marked_nodes);
    } else if (cur_node->node_->IsResourceNode()) {
      UpdateResourceNode(cur_node->node_, node_queue, marked_nodes);
    } else {
      LOG(FATAL) << "Unexpected node type: " << cur_node->node_->type_;
    }
    delete cur_node;
  }
}

void FlowGraphManager::UpdateResourceNode(
    FlowGraphNode* res_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(res_node);
  UpdateResOutgoingArcs(res_node, node_queue, marked_nodes);
}

void FlowGraphManager::UpdateResourceTopology(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  // TODO(ionel): We don't currently update the arc costs. Moreover, we should
  // handle the case when a resource's parent changes.
  const ResourceDescriptor& rd = rtnd_ptr->resource_desc();
  uint64_t old_capacity = CapacityFromResNodeToParent(rd);
  uint64_t old_num_slots = rd.num_slots_below();
  uint64_t old_num_running_tasks = rd.num_running_tasks_below();
  UpdateResourceTopologyDFS(rtnd_ptr);
  if (!rtnd_ptr->parent_id().empty()) {
    // We start from rtnd_ptr's parent because in UpdateResourceTopologyDFS
    // we already update the arc between rtnd_ptr and its parent.
    FlowGraphNode* cur_node =
      NodeForResourceID(ResourceIDFromString(rtnd_ptr->parent_id()));
    int64_t cap_delta = static_cast<int64_t>(CapacityFromResNodeToParent(rd)) -
      static_cast<int64_t>(old_capacity);
    int64_t num_slots_delta = static_cast<int64_t>(rd.num_slots_below()) -
      static_cast<int64_t>(old_num_slots);
    int64_t num_running_tasks_delta =
      static_cast<int64_t>(rd.num_running_tasks_below()) -
      static_cast<int64_t>(old_num_running_tasks);
    UpdateResourceStatsUpToRoot(cur_node, cap_delta, num_slots_delta,
                                num_running_tasks_delta);
  }
}

void FlowGraphManager::UpdateResourceTopologyDFS(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
  if (rd_ptr->type() == ResourceDescriptor::RESOURCE_PU) {
    // Base case.
    rd_ptr->set_num_slots_below(FLAGS_max_tasks_per_pu);
    rd_ptr->set_num_running_tasks_below(
        static_cast<uint64_t>(rd_ptr->current_running_tasks_size()));
  } else {
    rd_ptr->set_num_slots_below(0);
    rd_ptr->set_num_running_tasks_below(0);
  }
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         child_iter = rtnd_ptr->mutable_children()->pointer_begin();
       child_iter != rtnd_ptr->mutable_children()->pointer_end();
       ++child_iter) {
    UpdateResourceTopologyDFS(*child_iter);
    rd_ptr->set_num_slots_below(
         rd_ptr->num_slots_below() +
         (*child_iter)->resource_desc().num_slots_below());
    rd_ptr->set_num_running_tasks_below(
         rd_ptr->num_running_tasks_below() +
         (*child_iter)->resource_desc().num_running_tasks_below());
  }
  if (!rtnd_ptr->parent_id().empty()) {
    // Update the arc to the parent.
    FlowGraphNode* cur_node =
      NodeForResourceID(ResourceIDFromString(rd_ptr->uuid()));
    CHECK_NOTNULL(cur_node);
    FlowGraphNode* parent_node =
      FindPtrOrNull(node_to_parent_node_map_, cur_node);
    CHECK_NOTNULL(parent_node);
    FlowGraphArc* parent_arc =
      graph_change_manager_->mutable_flow_graph()->GetArc(parent_node,
                                                          cur_node);
    graph_change_manager_->ChangeArcCapacity(
        parent_arc, CapacityFromResNodeToParent(*rd_ptr), CHG_ARC_BETWEEN_RES,
        "UpdateResourceTopologyDFS");
  }
}

void FlowGraphManager::UpdateResourceStatsUpToRoot(
    FlowGraphNode* cur_node, int64_t cap_delta, int64_t slots_delta,
    int64_t running_tasks_delta) {
  CHECK_NOTNULL(cur_node);
  while (true) {
    FlowGraphNode* parent_node =
      FindPtrOrNull(node_to_parent_node_map_, cur_node);
    if (!parent_node) {
      // The node is the root of the topology.
      break;
    } else {
      FlowGraphArc* parent_arc =
        graph_change_manager_->mutable_flow_graph()->GetArc(parent_node,
                                                            cur_node);
      CHECK_NOTNULL(parent_arc);
      uint64_t new_capacity =
        static_cast<uint64_t>(
            static_cast<int64_t>(parent_arc->cap_upper_bound_) + cap_delta);
      graph_change_manager_->ChangeArcCapacity(
          parent_arc, new_capacity, CHG_ARC_BETWEEN_RES,
          "UpdateCapacityUpToRoot");
      uint64_t new_num_slots =
        static_cast<uint64_t>(
            static_cast<int64_t>(parent_node->rd_ptr_->num_slots_below()) +
            slots_delta);
      parent_node->rd_ptr_->set_num_slots_below(new_num_slots);
      uint64_t new_num_running_tasks =
        static_cast<uint64_t>(
            static_cast<int64_t>(
                 parent_node->rd_ptr_->num_running_tasks_below()) +
            running_tasks_delta);
      parent_node->rd_ptr_->set_num_running_tasks_below(new_num_running_tasks);
    }
    cur_node = parent_node;
  }
}

void FlowGraphManager::UpdateResOutgoingArcs(
    FlowGraphNode* res_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(res_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         res_node->outgoing_arc_map_.begin();
       it != res_node->outgoing_arc_map_.end(); ) {
    FlowGraphArc* arc = it->second;
    ++it;
    if (!arc->dst_node_->resource_id_.is_nil()) {
      ArcDescriptor arc_descriptor =
        cost_model_->ResourceNodeToResourceNode(*res_node->rd_ptr_,
                                                *arc->dst_node_->rd_ptr_);
      graph_change_manager_->ChangeArc(
          arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
          arc_descriptor.cost_, CHG_ARC_BETWEEN_RES,
          "UpdateResOutgoingArcs");
      if (marked_nodes->find(arc->dst_node_->id_) == marked_nodes->end()) {
        // Add the dst node to the queue if it hasn't been marked yet.
        marked_nodes->insert(arc->dst_node_->id_);
        node_queue->push(
            new TDOrNodeWrapper(arc->dst_node_, arc->dst_node_->td_ptr_));
      }
    } else {
      UpdateResToSinkArc(res_node);
    }
  }
}

void FlowGraphManager::UpdateResToSinkArc(FlowGraphNode* res_node) {
  if (res_node->type_ == FlowNodeType::PU) {
    CHECK_NOTNULL(sink_node_);
    FlowGraphArc* res_arc_sink =
      graph_change_manager_->mutable_flow_graph()->GetArc(res_node, sink_node_);
    ArcDescriptor arc_descriptor =
      cost_model_->LeafResourceNodeToSink(res_node->resource_id_);
    if (!res_arc_sink) {
      graph_change_manager_->AddArc(
          res_node, sink_node_, arc_descriptor.min_flow_,
          arc_descriptor.capacity_, arc_descriptor.cost_, OTHER,
          ADD_ARC_RES_TO_SINK, "UpdateResToSinkArc");

    } else {
      graph_change_manager_->ChangeArc(
          res_arc_sink, arc_descriptor.min_flow_, arc_descriptor.capacity_,
          arc_descriptor.cost_, CHG_ARC_RES_TO_SINK, "UpdateResToSinkArc");
    }
  } else {
    LOG(FATAL) << "Updating an arc from a non-PU to the sink";
  }
}

void FlowGraphManager::UpdateRunningTaskNode(
    FlowGraphNode* task_node,
    bool update_preferences,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(task_node);
  FlowGraphArc* running_arc = FindPtrOrNull(task_to_running_arc_,
                                            task_node->td_ptr_->uid());
  CHECK_NOTNULL(running_arc);
  ArcDescriptor arc_descriptor =
    cost_model_->TaskContinuation(task_node->td_ptr_->uid());
  graph_change_manager_->ChangeArc(
      running_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
      arc_descriptor.cost_, CHG_ARC_TASK_TO_RES,
      "UpdateRunningTaskNode: continuation cost");
  if (FLAGS_preemption) {
    UpdateRunningTaskToUnscheduledAggArc(task_node);
    if (update_preferences) {
      CHECK_NOTNULL(node_queue);
      CHECK_NOTNULL(marked_nodes);
      UpdateTaskToResArcs(task_node, node_queue, marked_nodes);
      UpdateTaskToEquivArcs(task_node, node_queue, marked_nodes);
    }
  }
}

void FlowGraphManager::UpdateRunningTaskToUnscheduledAggArc(
    FlowGraphNode* task_node) {
  CHECK(FLAGS_preemption) << "Arc to unscheduled doesn't exist for running task"
                          << " when preemption is not enabled";
  FlowGraphNode* unsched_agg_node = UnschedAggNodeForJobID(task_node->job_id_);
  CHECK_NOTNULL(unsched_agg_node);
  FlowGraphArc* unsched_arc =
    graph_change_manager_->mutable_flow_graph()->GetArc(task_node,
                                                        unsched_agg_node);
  CHECK_NOTNULL(unsched_arc);
  ArcDescriptor arc_descriptor =
    cost_model_->TaskPreemption(task_node->td_ptr_->uid());
  graph_change_manager_->ChangeArc(
      unsched_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
      arc_descriptor.cost_, CHG_ARC_TO_UNSCHED,
      "UpdateRunningTaskToUnscheduledAggArc");
}

void FlowGraphManager::UpdateTaskNode(FlowGraphNode* task_node,
                                      queue<TDOrNodeWrapper*>* node_queue,
                                      unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(task_node);
  if (task_node->IsTaskAssignedOrRunning()) {
    UpdateRunningTaskNode(task_node, FLAGS_update_preferences_running_task,
                          node_queue, marked_nodes);
  } else {
    UpdateTaskToUnscheduledAggArc(task_node);
    UpdateTaskToEquivArcs(task_node, node_queue, marked_nodes);
    UpdateTaskToResArcs(task_node, node_queue, marked_nodes);
  }
}

void FlowGraphManager::UpdateTaskToEquivArcs(
    FlowGraphNode* task_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(task_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  vector<EquivClass_t>* pref_ec =
    cost_model_->GetTaskEquivClasses(task_node->td_ptr_->uid());
  if (pref_ec) {
    for (auto& pref_ec_id : *pref_ec) {
      FlowGraphNode* pref_ec_node = NodeForEquivClass(pref_ec_id);
      if (!pref_ec_node) {
        pref_ec_node = AddEquivClassNode(pref_ec_id);
      }
      ArcDescriptor arc_descriptor =
        cost_model_->TaskToEquivClassAggregator(task_node->td_ptr_->uid(),
                                                pref_ec_id);
      FlowGraphArc* pref_ec_arc =
        graph_change_manager_->mutable_flow_graph()->GetArc(task_node,
                                                            pref_ec_node);
      if (!pref_ec_arc) {
        graph_change_manager_->AddArc(
            task_node, pref_ec_node, arc_descriptor.min_flow_,
            arc_descriptor.capacity_, arc_descriptor.cost_, OTHER,
            ADD_ARC_TASK_TO_EQUIV_CLASS, "UpdateTaskToEquivArcs");

      } else {
        graph_change_manager_->ChangeArc(
            pref_ec_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
            arc_descriptor.cost_, CHG_ARC_TASK_TO_EQUIV_CLASS,
            "UpdateTaskToEquivArcs");
      }
      if (marked_nodes->find(pref_ec_node->id_) == marked_nodes->end()) {
        // Add the EC node to the queue if it hasn't been marked yet.
        marked_nodes->insert(pref_ec_node->id_);
        node_queue->push(
            new TDOrNodeWrapper(pref_ec_node, pref_ec_node->td_ptr_));
      }
    }
    RemoveInvalidECPrefArcs(*task_node, *pref_ec, DEL_ARC_TASK_TO_EQUIV_CLASS);
    delete pref_ec;
  } else {
    vector<EquivClass_t> no_pref_ec;
    RemoveInvalidECPrefArcs(*task_node, no_pref_ec,
                            DEL_ARC_TASK_TO_EQUIV_CLASS);
  }
}

void FlowGraphManager::UpdateTaskToResArcs(
    FlowGraphNode* task_node,
    queue<TDOrNodeWrapper*>* node_queue,
    unordered_set<uint64_t>* marked_nodes) {
  CHECK_NOTNULL(task_node);
  CHECK_NOTNULL(node_queue);
  CHECK_NOTNULL(marked_nodes);
  vector<ResourceID_t>* pref_res =
    cost_model_->GetTaskPreferenceArcs(task_node->td_ptr_->uid());
  if (pref_res) {
    for (auto& pref_res_id : *pref_res) {
      FlowGraphNode* pref_res_node = NodeForResourceID(pref_res_id);
      // The resource node should already exist because the cost models cannot
      // prefer a resource before it is added to the graph.
      CHECK_NOTNULL(pref_res_node);
      ArcDescriptor arc_descriptor =
        cost_model_->TaskToResourceNode(task_node->td_ptr_->uid(), pref_res_id);
      FlowGraphArc* pref_res_arc =
        graph_change_manager_->mutable_flow_graph()->GetArc(task_node,
                                                            pref_res_node);
      if (!pref_res_arc) {
        graph_change_manager_->AddArc(
            task_node, pref_res_node, arc_descriptor.min_flow_,
            arc_descriptor.capacity_, arc_descriptor.cost_, OTHER,
            ADD_ARC_TASK_TO_RES, "UpdateTaskToResArcs");
      } else if (pref_res_arc->type_ != FlowGraphArcType::RUNNING) {
        // We don't change the cost of the arc if it's a running arc because
        // the arc is updated somewhere else. Moreover, the cost of running
        // arcs is returned by TaskContinuation.
        graph_change_manager_->ChangeArcCost(pref_res_arc, arc_descriptor.cost_,
                                             CHG_ARC_TASK_TO_RES,
                                             "UpdateTaskToResArcs");
      }
      if (marked_nodes->find(pref_res_node->id_) == marked_nodes->end()) {
        // Add the res node to the queue if it hasn't been marked yet.
        marked_nodes->insert(pref_res_node->id_);
        node_queue->push(
            new TDOrNodeWrapper(pref_res_node, pref_res_node->td_ptr_));
      }
    }
    RemoveInvalidPrefResArcs(*task_node, *pref_res, DEL_ARC_TASK_TO_RES);
    delete pref_res;
  } else {
    vector<ResourceID_t> no_pref_res;
    RemoveInvalidPrefResArcs(*task_node, no_pref_res, DEL_ARC_TASK_TO_RES);
  }
}

FlowGraphNode* FlowGraphManager::UpdateTaskToUnscheduledAggArc(
    FlowGraphNode* task_node) {
  CHECK_NOTNULL(task_node);
  FlowGraphNode* unsched_agg_node = UnschedAggNodeForJobID(task_node->job_id_);
  if (!unsched_agg_node) {
    unsched_agg_node = AddUnscheduledAggNode(task_node->job_id_);
  }
  ArcDescriptor arc_descriptor =
    cost_model_->TaskToUnscheduledAgg(task_node->td_ptr_->uid());
  FlowGraphArc* to_unsched_arc =
    graph_change_manager_->mutable_flow_graph()->GetArc(task_node,
                                                        unsched_agg_node);
  if (!to_unsched_arc) {
    graph_change_manager_->AddArc(
        task_node, unsched_agg_node, arc_descriptor.min_flow_,
        arc_descriptor.capacity_, arc_descriptor.cost_, OTHER,
        ADD_ARC_TO_UNSCHED, "UpdateTaskToUnscheduledAggArc");
  } else {
    graph_change_manager_->ChangeArc(
        to_unsched_arc, arc_descriptor.min_flow_, arc_descriptor.capacity_,
        arc_descriptor.cost_, CHG_ARC_TO_UNSCHED,
        "UpdateTaskToUnscheduledAggArc");
  }
  return unsched_agg_node;
}

void FlowGraphManager::UpdateTimeDependentCosts(
    const vector<JobDescriptor*>& jd_ptr_vec) {
  AddOrUpdateJobNodes(jd_ptr_vec);
}

void FlowGraphManager::UpdateUnscheduledAggNode(
    FlowGraphNode* unsched_agg_node, int64_t cap_delta) {
  CHECK_NOTNULL(unsched_agg_node);
  FlowGraphArc* unsched_agg_sink_arc =
    graph_change_manager_->mutable_flow_graph()->GetArc(unsched_agg_node,
                                                        sink_node_);
  ArcDescriptor arc_descriptor =
    cost_model_->UnscheduledAggToSink(unsched_agg_node->job_id_);
  // TODO(ionel): Compute cap_delta and new_capacity in the cost_models.
  if (!unsched_agg_sink_arc) {
    CHECK_GE(cap_delta, 1);
    graph_change_manager_->AddArc(
        unsched_agg_node, sink_node_, arc_descriptor.min_flow_,
        static_cast<uint64_t>(cap_delta), arc_descriptor.cost_, OTHER,
        ADD_ARC_FROM_UNSCHED, "UpdateUnscheduledAggNode");
  } else {
    int64_t cap_upper_bound =
      static_cast<int64_t>(unsched_agg_sink_arc->cap_upper_bound_);
    uint64_t new_capacity = static_cast<uint64_t>(cap_upper_bound + cap_delta);
    graph_change_manager_->ChangeArc(
        unsched_agg_sink_arc, arc_descriptor.min_flow_,
        new_capacity, arc_descriptor.cost_, CHG_ARC_FROM_UNSCHED,
        "UpdateUnscheduledAggNode");
  }
}

void FlowGraphManager::VisitTopologyChildren(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
  CHECK_NOTNULL(rtnd_ptr);
  ResourceDescriptor* rd_ptr = rtnd_ptr->mutable_resource_desc();
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         child_iter = rtnd_ptr->mutable_children()->pointer_begin();
       child_iter != rtnd_ptr->mutable_children()->pointer_end();
       ++child_iter) {
    AddResourceTopologyDFS(*child_iter);
    rd_ptr->set_num_slots_below(
         rd_ptr->num_slots_below() +
         (*child_iter)->resource_desc().num_slots_below());
    rd_ptr->set_num_running_tasks_below(
         rd_ptr->num_running_tasks_below() +
         (*child_iter)->resource_desc().num_running_tasks_below());
  }
}

}  // namespace firmament
