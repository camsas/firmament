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

#include "scheduling/flow/flow_graph_arc.h"

namespace firmament {

  FlowGraphArc::FlowGraphArc(uint64_t src, uint64_t dst,
                             FlowGraphNode* src_node,
                             FlowGraphNode* dst_node)
      : src_(src), dst_(dst), cap_lower_bound_(0),
        cap_upper_bound_(0), cost_(0), src_node_(src_node),
        dst_node_(dst_node), type_(OTHER) {}
  FlowGraphArc::FlowGraphArc(uint64_t src, uint64_t dst, uint64_t clb,
                             uint64_t cub, int64_t cost,
                             FlowGraphNode* src_node, FlowGraphNode* dst_node)
      : src_(src), dst_(dst), cap_lower_bound_(clb), cap_upper_bound_(cub),
        cost_(cost), src_node_(src_node), dst_node_(dst_node), type_(OTHER) {
  }
} // namespace firmament
