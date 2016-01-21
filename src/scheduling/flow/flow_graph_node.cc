// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>

#include "scheduling/flow/flow_graph_node.h"

#include "base/common.h"
#include "misc/map-util.h"

namespace firmament {

  FlowGraphNode::FlowGraphNode(uint64_t id)
      : id_(id), excess_(0), job_id_(boost::uuids::nil_uuid()),
        resource_id_(boost::uuids::nil_uuid()), rd_ptr_(NULL), task_id_(0),
        visited_(0) {
  }

  FlowGraphNode::FlowGraphNode(uint64_t id, uint64_t excess)
      : id_(id), excess_(excess), job_id_(boost::uuids::nil_uuid()),
        resource_id_(boost::uuids::nil_uuid()), rd_ptr_(NULL), task_id_(0),
        visited_(0) {
  }

  void FlowGraphNode::AddArc(FlowGraphArc* arc) {
    CHECK_EQ(arc->src_, id_);
    CHECK(InsertIfNotPresent(&outgoing_arc_map_, arc->dst_, arc));
    CHECK(InsertIfNotPresent(&(arc->dst_node_->incoming_arc_map_),
                             arc->src_, arc));
  }

} // namespace firmament
