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

#include <string>
#include <vector>

#include "scheduling/flow/dimacs_add_node.h"

namespace firmament {

// Node type is used to construct the mapping of tasks to PUs in the solver.
// NOTE: Do not reorder types because it will affect the communication with
// the solver.

enum NodeType {
  DIMACS_NODE_OTHER = 0,
  DIMACS_NODE_TASK = 1,
  DIMACS_NODE_PU = 2,
  DIMACS_NODE_SINK = 3,
  DIMACS_NODE_MACHINE = 4,
  DIMACS_NODE_INTERMEDIATE_RES = 5
};

DIMACSAddNode::DIMACSAddNode(const FlowGraphNode& node,
                             const vector<FlowGraphArc*>& arcs) :
     DIMACSChange(), id_(node.id_), excess_(node.excess_), type_(node.type_) {
  for (FlowGraphArc* arc : arcs) {
    // NOTE: The DIMACS stats for these new arcs have already been updated when
    // the arcs were created.
    arc_additions_.push_back(DIMACSNewArc(*arc));
  }
}

const string DIMACSAddNode::GenerateChange() const {
  stringstream ss;
  ss << DIMACSChange::GenerateChangeDescription();
  ss << "n " << id_ << " " << excess_ << " " << GetNodeType() << "\n";
  for (const DIMACSNewArc &new_arc : arc_additions_) {
    ss << new_arc.GenerateChange();
  }
  return ss.str();
}

uint32_t DIMACSAddNode::GetNodeType() const {
  if (type_ == FlowNodeType::PU) {
    return DIMACS_NODE_PU;
  } else if (type_ == FlowNodeType::MACHINE) {
    return DIMACS_NODE_MACHINE;
  } else if (type_ == FlowNodeType::NUMA_NODE ||
             type_ == FlowNodeType::SOCKET ||
             type_ == FlowNodeType::CACHE ||
             type_ == FlowNodeType::CORE) {
    return DIMACS_NODE_INTERMEDIATE_RES;
  } else if (type_ == FlowNodeType::SINK) {
    return DIMACS_NODE_SINK;
  } else if (type_ == FlowNodeType::UNSCHEDULED_TASK ||
             type_ == FlowNodeType::SCHEDULED_TASK ||
             type_ == FlowNodeType::ROOT_TASK) {
    return DIMACS_NODE_TASK;
  } else {
    return DIMACS_NODE_OTHER;
  }
}

} // namespace firmament
