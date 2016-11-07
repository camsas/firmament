/*
 * Firmament
 * Copyright (c) Ionel Gog <ionel.gog@cl.cam.ac.uk>
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

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H

#include <string>
#include <vector>

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

class DIMACSAddNode : public DIMACSChange {
 public:
  DIMACSAddNode(const FlowGraphNode& node, const vector<FlowGraphArc*>& arcs);
  ~DIMACSAddNode() {}
  const string GenerateChange() const;
  uint32_t GetNodeType() const;
  const uint64_t id_;
  const int64_t excess_;
  const FlowNodeType type_;
  vector<DIMACSNewArc> arc_additions_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H
