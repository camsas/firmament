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
               int64_t cost, FlowGraphNode* src_node, FlowGraphNode* dst_node);

  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  int64_t cost_;
  FlowGraphNode* src_node_;
  FlowGraphNode* dst_node_;
  FlowGraphArcType type_;
};

} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_ARC_H
