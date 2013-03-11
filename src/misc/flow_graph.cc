// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#include <string>
#include <queue>

#include <cstdio>

#include <boost/bind.hpp>

#include "misc/flow_graph.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/utils.h"

namespace firmament {

using machine::topology::TopologyManager;

FlowGraph::FlowGraph()
    : current_id_(1) {
  // Add sink and cluster aggregator node
  AddSpecialNodes();
}

void FlowGraph::AddArcsForTask(TaskDescriptor* cur,
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
  return arc;
}

FlowGraphArc* FlowGraph::AddArcInternal(FlowGraphNode* src,
                                        FlowGraphNode* dst) {
  FlowGraphArc* arc = new FlowGraphArc(src->id_, dst->id_);
  arc_set_.insert(arc);
  return arc;
}

void FlowGraph::AddJobNodes(JobDescriptor* jd) {
  // First add an unscheduled aggregator node for this job
  FlowGraphNode* unsched_agg_node = AddNodeInternal(next_id());
  // ... and connect it directly to the sink
  FlowGraphArc* unsched_agg_to_sink_arc =
      AddArcInternal(unsched_agg_node, sink_node_);
  unsched_agg_to_sink_arc->cap_upper_bound_ = 0;
  // Now add the job's task nodes
  // XXX(malte): This is a simple BFS lashup
  queue<TaskDescriptor*> q;
  q.push(jd->mutable_root_task());
  while (!q.empty()) {
    TaskDescriptor* cur = q.front();
    q.pop();
    // Add the current task's node
    FlowGraphNode* task_node = AddNodeInternal(next_id());
    task_node->supply_ = 1;
    sink_node_->demand_++;
    // Arcs for this node
    AddArcsForTask(cur, task_node, unsched_agg_node, unsched_agg_to_sink_arc);
    // Enqueue any existing children of this task
    for (RepeatedPtrField<TaskDescriptor>::iterator c_iter =
         cur->mutable_spawned()->begin();
         c_iter != cur->mutable_spawned()->end();
         ++c_iter) {
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
  // Sink node
  sink_node_ = AddNodeInternal(next_id());
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
    // Root node, so add it
    VLOG(2) << "Adding node for root resource "
            << rtnd->resource_desc().uuid();
    FlowGraphNode* root_node = AddNodeInternal(next_id());
    InsertIfNotPresent(&resource_to_nodeid_map_,
                       ResourceIDFromString(rtnd->resource_desc().uuid()),
                       root_node->id_);
    // Arc from cluster aggregator to resource topo root node
    cluster_agg_into_res_topo_arc_ = AddArcInternal(cluster_agg_node_,
                                                    root_node);
  }
  if (rtnd->children_size() > 0) {
    VLOG(2) << "Adding " << rtnd->children_size() << " internal resource arcs";
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator c_iter =
         rtnd->mutable_children()->begin();
         c_iter != rtnd->mutable_children()->end();
         ++c_iter) {
      VLOG(2) << "Adding node for resource " << c_iter->resource_desc().uuid();
      FlowGraphNode* child_node = AddNodeInternal(next_id());
      InsertIfNotPresent(&resource_to_nodeid_map_,
                         ResourceIDFromString(c_iter->resource_desc().uuid()),
                         child_node->id_);
      // If we do not have a parent_id set, this is a root node, so it has no
      // incoming internal resource topology edges
      if (c_iter->has_parent_id()) {
        FlowGraphNode* cur_node = NodeForResourceID(
            ResourceIDFromString(c_iter->parent_id()));
        AddArcInternal(cur_node, child_node);
      } else {
        LOG(ERROR) << "Found child without parent_id set! This will lead to an "
                   << "inconsistent flow graph!";
      }
    }
  } else {
    // Leaves of the resource topology; add an arc to the sink node
    VLOG(2) << "Adding arc from leaf resource " << rtnd->resource_desc().uuid()
            << " to sink node.";
    if (rtnd->resource_desc().type() != ResourceDescriptor::RESOURCE_PU)
      LOG(ERROR) << "Leaf resource " << rtnd->resource_desc().uuid()
                 << " is not a PU! This may yield an unschedulable flow!";
    FlowGraphNode* cur_node = NodeForResourceID(
        ResourceIDFromString(rtnd->resource_desc().uuid()));
    AddArcInternal(cur_node->id_, sink_node_->id_);
    (*leaf_counter)++;
  }
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

}  // namespace firmament
