// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>

#include "scheduling/flow/flow_graph_node.h"

#include "base/common.h"
#include "misc/map-util.h"

namespace firmament {

  FlowGraphNode::FlowGraphNode(uint64_t id)
      : id_(id), excess_(0), job_id_(boost::uuids::nil_uuid()),
        resource_id_(boost::uuids::nil_uuid()), rd_ptr_(NULL), td_ptr_(NULL),
        ec_id_(0), visited_(0) {
  }

  FlowGraphNode::FlowGraphNode(uint64_t id, int64_t excess)
      : id_(id), excess_(excess), job_id_(boost::uuids::nil_uuid()),
        resource_id_(boost::uuids::nil_uuid()), rd_ptr_(NULL), td_ptr_(NULL),
        ec_id_(0), visited_(0) {
  }

  void FlowGraphNode::AddArc(FlowGraphArc* arc) {
    CHECK_EQ(arc->src_, id_);
    CHECK(InsertIfNotPresent(&outgoing_arc_map_, arc->dst_, arc));
    CHECK(InsertIfNotPresent(&(arc->dst_node_->incoming_arc_map_),
                             arc->src_, arc));
  }

  FlowNodeType FlowGraphNode::TransformToResourceNodeType(
      const ResourceDescriptor& rd) {
    switch (rd.type()) {
      case ResourceDescriptor::RESOURCE_PU: return FlowNodeType::PU;
      case ResourceDescriptor::RESOURCE_CORE: return FlowNodeType::CORE;
      case ResourceDescriptor::RESOURCE_CACHE: return FlowNodeType::CACHE;
      case ResourceDescriptor::RESOURCE_NIC:
        LOG(FATAL) << "Node type not supported yet: " << rd.type();
      case ResourceDescriptor::RESOURCE_DISK:
        LOG(FATAL) << "Node type not supported yet: " << rd.type();
      case ResourceDescriptor::RESOURCE_SSD:
        LOG(FATAL) << "Node type not supported yet: " << rd.type();
      case ResourceDescriptor::RESOURCE_MACHINE: return FlowNodeType::MACHINE;
      case ResourceDescriptor::RESOURCE_LOGICAL:
        LOG(FATAL) << "Node type not supported yet: " << rd.type();
      case ResourceDescriptor::RESOURCE_NUMA_NODE:
        return FlowNodeType::NUMA_NODE;
      case ResourceDescriptor::RESOURCE_SOCKET: return FlowNodeType::SOCKET;
      case ResourceDescriptor::RESOURCE_COORDINATOR:
        return FlowNodeType::COORDINATOR;
      default:
        LOG(FATAL) << "Unknown node type: " << rd.type();
    }
  }

} // namespace firmament
