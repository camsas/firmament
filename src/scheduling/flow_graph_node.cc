// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>

#include "base/common.h"
#include "misc/map-util.h"
#include "scheduling/flow_graph_node.h"

namespace firmament {

  FlowGraphNode::FlowGraphNode(uint64_t id)
      : id_(id), excess_(0), resource_id_(boost::uuids::nil_uuid()),
    task_id_(0) {
  }

  FlowGraphNode::FlowGraphNode(uint64_t id, uint64_t excess)
      : id_(id), excess_(excess), resource_id_(boost::uuids::nil_uuid()),
    task_id_(0) {
  }

  void FlowGraphNode::AddArc(FlowGraphArc* arc) {
    CHECK_EQ(arc->src_, id_);
    InsertIfNotPresent(&outgoing_arc_map_, arc->dst_, arc);
    InsertIfNotPresent(&arc->dst_node_->incoming_arc_map_, arc->src_, arc);
  }

} // namespace firmament
