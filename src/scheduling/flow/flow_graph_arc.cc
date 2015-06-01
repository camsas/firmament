// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>

#include "scheduling/flow/flow_graph_arc.h"

namespace firmament {

  FlowGraphArc::FlowGraphArc(uint64_t src, uint64_t dst,
                             FlowGraphNode* src_node,
                             FlowGraphNode* dst_node)
      : src_(src), dst_(dst), cap_lower_bound_(0),
        cap_upper_bound_(0), cost_(0), src_node_(src_node),
        dst_node_(dst_node) {}
  FlowGraphArc::FlowGraphArc(uint64_t src, uint64_t dst, uint64_t clb,
                             uint64_t cub, uint64_t cost,
                             FlowGraphNode* src_node, FlowGraphNode* dst_node)
      : src_(src), dst_(dst), cap_lower_bound_(clb), cap_upper_bound_(cub),
        cost_(cost), src_node_(src_node), dst_node_(dst_node) {
  }
} // namespace firmament
