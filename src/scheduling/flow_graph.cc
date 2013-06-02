// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#include <string>
#include <queue>

#include <cstdio>

#include <boost/bind.hpp>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/string_utils.h"
#include "misc/utils.h"
#include "scheduling/flow_graph.h"

namespace firmament {

using machine::topology::TopologyManager;

FlowGraph::FlowGraph()
    : current_id_(1) {
  // Add sink and cluster aggregator node
  AddSpecialNodes();
}

void FlowGraph::AddArcsForTask(TaskDescriptor*,
                               FlowGraphNode* task_node,
                               FlowGraphNode* unsched_agg_node,
                               FlowGraphArc* unsched_agg_to_sink_arc) {
  // We always have an edge to the cluster aggregator node
  AddArcInternal(task_node, cluster_agg_node_);
  // We also always have an edge to our job's unscheduled node
  FlowGraphArc* unsched_arc = AddArcInternal(task_node, unsched_agg_node);
  // TODO(malte): stub, read value from runtime config here
  unsched_agg_to_sink_arc->cap_upper_bound_++;
  unsched_arc->cost_ = 1;
}

FlowGraphArc* FlowGraph::AddArcInternal(uint64_t src, uint64_t dst) {
  FlowGraphArc* arc = new FlowGraphArc(src, dst);
  arc_set_.insert(arc);
  FlowGraphNode** src_node = FindOrNull(node_map_, src);
  CHECK_NOTNULL(src_node);
  (*src_node)->AddArc(arc);
  return arc;
}

FlowGraphArc* FlowGraph::AddArcInternal(FlowGraphNode* src,
                                        FlowGraphNode* dst) {
  FlowGraphArc* arc = new FlowGraphArc(src->id_, dst->id_);
  arc_set_.insert(arc);
  src->AddArc(arc);
  return arc;
}

void FlowGraph::AddJobNodes(JobDescriptor* jd) {
  // First add an unscheduled aggregator node for this job
  // if none exists alread
  FlowGraphArc* unsched_agg_to_sink_arc;
  FlowGraphNode* unsched_agg_node;
  uint64_t* unsched_agg_node_id = FindOrNull(job_to_nodeid_map_,
                                             JobIDFromString(jd->uuid()));
  if (!unsched_agg_node_id) {
    unsched_agg_node = AddNodeInternal(next_id());
    unsched_agg_node->type_.set_type(FlowNodeType::JOB_AGGREGATOR);
    string comment;
    spf(&comment, "UNSCHED AGG for %s", jd->uuid().c_str());
    unsched_agg_node->comment_ = comment;
    // ... and connect it directly to the sink
    unsched_agg_to_sink_arc = AddArcInternal(unsched_agg_node, sink_node_);
    unsched_agg_to_sink_arc->cap_upper_bound_ = 0;
    // Record this for the future in the job <-> node ID lookup table
    CHECK(InsertIfNotPresent(&job_to_nodeid_map_, JobIDFromString(jd->uuid()),
                             unsched_agg_node->id_));
  } else {
    FlowGraphNode** unsched_agg_node_ptr = FindOrNull(node_map_,
                                                      *unsched_agg_node_id);
    unsched_agg_node = *unsched_agg_node_ptr;
    FlowGraphArc** lookup_ptr = FindOrNull(unsched_agg_node->outgoing_arc_map_,
                                           sink_node_->id_);
    CHECK_NOTNULL(lookup_ptr);
    unsched_agg_to_sink_arc = *lookup_ptr;
  }
  // Now add the job's task nodes
  // TODO(malte): This is a simple BFS lashup; maybe we can do better?
  queue<TaskDescriptor*> q;
  q.push(jd->mutable_root_task());
  while (!q.empty()) {
    TaskDescriptor* cur = q.front();
    q.pop();
    // Check if this node has already been added
    uint64_t* tn_ptr = FindOrNull(task_to_nodeid_map_, cur->uid());
    FlowGraphNode* task_node = tn_ptr ? Node(*tn_ptr) : NULL;
    if (cur->state() == TaskDescriptor::RUNNABLE && !task_node) {
      task_node = AddNodeInternal(next_id());
      task_node->type_.set_type(FlowNodeType::UNSCHEDULED_TASK); 
      // Add the current task's node
      task_node->supply_ = 1;
      task_node->task_id_ = cur->uid();  // set task ID in node
      sink_node_->demand_++;
      task_nodes_.insert(task_node->id_);
      // Insert a record for the node representing this task's ID
      InsertIfNotPresent(&task_to_nodeid_map_, cur->uid(), task_node->id_);
      // Log info
      VLOG(2) << "Adding edges for task " << cur->uid() << "'s node ("
              << task_node->id_ << "); task state is " << cur->state();
      // Arcs for this node
      AddArcsForTask(cur, task_node, unsched_agg_node, unsched_agg_to_sink_arc);
    } else if (cur->state() == TaskDescriptor::RUNNING ||
             cur->state() == TaskDescriptor::ASSIGNED) {
      // The task is already running, so it must have a node already
      //task_node->type_.set_type(FlowNodeType::SCHEDULED_TASK);
    } else if (task_node) {
      VLOG(2) << "Ignoring task " << cur->uid()
              << ", as its node already exists.";
    } else {
      VLOG(2) << "Ignoring task " << cur->uid() << " [" << hex << cur
              << "], which is in state "
              << ENUM_TO_STRING(TaskDescriptor::TaskState, cur->state());
    }
    // Enqueue any existing children of this task
    for (RepeatedPtrField<TaskDescriptor>::iterator c_iter =
         cur->mutable_spawned()->begin();
         c_iter != cur->mutable_spawned()->end();
         ++c_iter) {
      // We do actually need to push tasks even if they are already completed,
      // failed or running, since they may have children eligible for
      // scheduling.
      q.push(&(*c_iter));
    }
  }
  // Set the supply on the unscheduled node to the difference between the
  // maximum number of running tasks for this job and the number of tasks
  // (F_j - N_j in Quincy terms).
  // TODO(malte): Stub -- this currently allows an unlimited number of tasks per
  // job to be scheduled.
  unsched_agg_node->supply_ = 0;
}

FlowGraphNode* FlowGraph::AddNodeInternal(uint64_t id) {
  FlowGraphNode* node = new FlowGraphNode(id);
  CHECK(InsertIfNotPresent(&node_map_, id, node));
  return node;
}

void FlowGraph::AddSpecialNodes() {
  // Cluster aggregator node X
  cluster_agg_node_ = AddNodeInternal(next_id());
  cluster_agg_node_->type_.set_type(FlowNodeType::GLOBAL_AGGREGATOR);
  cluster_agg_node_->comment_ = "CLUSTER AGG";
  // Sink node
  sink_node_ = AddNodeInternal(next_id());
  sink_node_->type_.set_type(FlowNodeType::SINK);
  sink_node_->comment_ = "SINK";
}

void FlowGraph::AddResourceTopology(
    ResourceTopologyNodeDescriptor* resource_tree) {
  uint64_t num_leaves = 0;
  TraverseResourceProtobufTreeReturnRTND(
      resource_tree,
      boost::bind(&FlowGraph::AddResourceNode, this, _1, &num_leaves));
  VLOG(2) << "Added a total of " << num_leaves << " schedulable (PU) "
          << " resources to flow graph; setting cluster aggregation node"
          << " output capacity accordingly.";
  cluster_agg_into_res_topo_arc_->cap_upper_bound_ = num_leaves;
}

void FlowGraph::AddResourceNode(ResourceTopologyNodeDescriptor* rtnd,
                                uint64_t* leaf_counter) {
  if (!rtnd->has_parent_id()) {
    // 1) Root node
    uint64_t id = next_id();
    VLOG(2) << "Adding node " << id << " for root resource "
            << rtnd->resource_desc().uuid();
    FlowGraphNode* root_node = AddNodeInternal(id);
    root_node->type_.set_type(FlowNodeType::MACHINE);
    InsertIfNotPresent(&resource_to_nodeid_map_,
                       ResourceIDFromString(rtnd->resource_desc().uuid()),
                       root_node->id_);
    // Arc from cluster aggregator to resource topo root node
    cluster_agg_into_res_topo_arc_ = AddArcInternal(cluster_agg_node_,
                                                    root_node);
  }
  if (rtnd->children_size() > 0) {
    // 2) Node inside the tree with non-zero children (i.e. no leaf node)
    VLOG(2) << "Adding " << rtnd->children_size() << " internal resource arcs";
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator c_iter =
         rtnd->mutable_children()->begin();
         c_iter != rtnd->mutable_children()->end();
         ++c_iter) {
      uint64_t id = next_id();
      VLOG(2) << "Adding node " << id << " for resource "
              << c_iter->resource_desc().uuid();
      FlowGraphNode* child_node = AddNodeInternal(id);
      child_node->resource_id_ = ResourceIDFromString(
          c_iter->resource_desc().uuid());
      InsertIfNotPresent(&resource_to_nodeid_map_,
                         ResourceIDFromString(c_iter->resource_desc().uuid()),
                         child_node->id_);
      // If we do not have a parent_id set, this is a root node, so it has no
      // incoming internal resource topology edges
      if (c_iter->has_parent_id()) {
        FlowGraphNode* cur_node = NodeForResourceID(
            ResourceIDFromString(c_iter->parent_id()));
        CHECK(cur_node != NULL) << "Could not find parent node with ID "
                                << c_iter->parent_id();
        AddArcInternal(cur_node, child_node);
        // XXX(malte): Need to assign correct capacity here!
      } else {
        LOG(ERROR) << "Found child without parent_id set! This will lead to an "
                   << "inconsistent flow graph!";
      }
    }
  } else {
    // 3) Leaves of the resource topology; add an arc to the sink node
    VLOG(2) << "Adding arc from leaf resource " << rtnd->resource_desc().uuid()
            << " to sink node.";
    if (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_PU)
      LOG(ERROR) << "Leaf resource " << rtnd->resource_desc().uuid()
                 << " is not a PU! This may yield an unschedulable flow!";
    FlowGraphNode* cur_node = NodeForResourceID(
        ResourceIDFromString(rtnd->resource_desc().uuid()));
    CHECK(cur_node != NULL) << "Could not find leaf node with ID "
                            << rtnd->resource_desc().uuid();
    cur_node->type_.set_type(FlowNodeType::PU);
    AddArcInternal(cur_node->id_, sink_node_->id_);
    leaf_nodes_.insert(cur_node->id_);
    (*leaf_counter)++;
  }
}

void FlowGraph::DeleteArc(FlowGraphArc* arc) {
  // First remove various meta-data relating to this arc
  arc_set_.erase(arc);
  // Then delete the arc itself
  delete arc;
}

void FlowGraph::DeleteTaskNode(FlowGraphNode* node) {
  // First remove all outgoing arcs
  for(unordered_map<uint64_t, FlowGraphArc*>::iterator it =
      node->outgoing_arc_map_.begin();
      it != node->outgoing_arc_map_.end();
      ++it) {
    DeleteArc(it->second);
  }
  node->outgoing_arc_map_.clear();
  // Decrease the sink's demand and set this node's supply to zero
  node->supply_ = 0;
  sink_node_->demand_--;
  // Find the unscheduled node for this job and decrement its outgoing capacity
  // TODO
  // Then remove node meta-data
  //node_map_.erase(node->id_);
  //task_nodes_.erase()
  // Then remove the node itself
  //delete node;
}

FlowGraphNode* FlowGraph::NodeForResourceID(const ResourceID_t& res_id) {
  uint64_t* id = FindOrNull(resource_to_nodeid_map_, res_id);
  // Returns NULL if resource unknown
  if (!id)
    return NULL;
  VLOG(2) << "Resource " << res_id << " is represented by node " << *id;
  FlowGraphNode** node_ptr = FindOrNull(node_map_, *id);
  return (node_ptr ? *node_ptr : NULL);
}

FlowGraphNode* FlowGraph::NodeForTaskID(TaskID_t task_id) {
  uint64_t* id = FindOrNull(task_to_nodeid_map_, task_id);
  // Returns NULL if task unknown
  if (!id)
    return NULL;
  VLOG(2) << "Task " << task_id << " is represented by node " << *id;
  FlowGraphNode** node_ptr = FindOrNull(node_map_, *id);
  return (node_ptr ? *node_ptr : NULL);
}

void FlowGraph::PinTaskToNode(FlowGraphNode* task_node,
                              FlowGraphNode* res_node) {
  // Remove all arcs apart from the task -> resource mapping;
  // note that this effectively disables preemption!
  for (unordered_map<TaskID_t, FlowGraphArc*>::iterator it =
       task_node->outgoing_arc_map_.begin();
       it != task_node->outgoing_arc_map_.end();
       ++it) {
    VLOG(2) << "Deleting arc from " << it->second->src_ << " to "
            << it->second->dst_;
    // N.B. This is a little dodgy, as it mutates the collection inside the
    // loop. However, since nobody else is reading from it at the same time,
    // this should be fine.
    task_node->outgoing_arc_map_.erase(it->first);
    DeleteArc(it->second);
  }
  // Re-add a single arc from the task to the resource node
  AddArcInternal(task_node, res_node);
}

void FlowGraph::UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id) {
  FlowGraphNode* task_node = NodeForTaskID(tid);
  FlowGraphNode* assigned_res_node = NodeForResourceID(res_id);
  CHECK_NOTNULL(task_node);
  CHECK_NOTNULL(assigned_res_node);
  PinTaskToNode(task_node, assigned_res_node);
}

}  // namespace firmament
