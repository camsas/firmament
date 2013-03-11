// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#include <string>

#include <cstdio>

#include <boost/bind.hpp>

#include "misc/flow_graph.h"
#include "misc/map-util.h"
#include "misc/pb_utils.h"
#include "misc/utils.h"

namespace firmament {

using machine::topology::TopologyManager;

FlowGraph::FlowGraph()
    : current_id_(0) {
  // Add sink and cluster aggregator node
  AddSpecialNodes();
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
  TraverseResourceProtobufTreeReturnRTND(
      resource_tree,
      boost::bind(&FlowGraph::AddResourceNode, this, _1));
}

void FlowGraph::AddResourceNode(ResourceTopologyNodeDescriptor* rtnd) {
  if (!rtnd->has_parent_id()) {
    // Root node, so add it
    VLOG(2) << "Adding node for root resource "
            << rtnd->resource_desc().uuid();
    FlowGraphNode* root_node = AddNodeInternal(next_id());
    InsertIfNotPresent(&resource_to_nodeid_map_,
                       ResourceIDFromString(rtnd->resource_desc().uuid()),
                       root_node->id_);
  }
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
