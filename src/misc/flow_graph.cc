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

FlowGraph::FlowGraph() {
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
  VLOG(1) << "Adding " << rtnd->children_size() << " internal resource arcs";
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator c_iter =
       rtnd->mutable_children()->begin();
       c_iter != rtnd->mutable_children()->end();
       ++c_iter) {
    VLOG(1) << "Adding node for resource " << rtnd->resource_desc().uuid();
    FlowGraphNode* cur_node = NodeForResourceID(
        ResourceIDFromString(rtnd->parent_id()));
    FlowGraphNode* child_node = AddNodeInternal(next_id());
    AddArcInternal(cur_node, child_node);
  }
}

FlowGraphNode* FlowGraph::NodeForResourceID(const ResourceID_t& res_id) {
  // stub
  return NULL;
}

}  // namespace firmament
