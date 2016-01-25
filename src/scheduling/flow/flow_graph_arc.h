// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Represents an arc in the scheduling flow graph.

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_ARC_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_ARC_H

#include <string>

#include "base/common.h"
#include "base/types.h"

namespace firmament {

// Forward declaration.
struct FlowGraphNode;

enum FlowGraphArcType {
  OTHER = 0,
  RUNNING = 1,
};

struct FlowGraphArc {
  FlowGraphArc(uint64_t src, uint64_t dst, FlowGraphNode* src_node,
               FlowGraphNode* dst_node);
  FlowGraphArc(uint64_t src, uint64_t dst, uint64_t clb, uint64_t cub,
               uint64_t cost, FlowGraphNode* src_node, FlowGraphNode* dst_node);

  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  uint64_t cost_;
  FlowGraphNode* src_node_;
  FlowGraphNode* dst_node_;
  FlowGraphArcType type_;
};

} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_ARC_H
