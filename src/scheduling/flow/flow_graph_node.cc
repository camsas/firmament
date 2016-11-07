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
