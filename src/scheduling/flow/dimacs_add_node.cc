// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
  DIMACS_NODE_SINK = 3
};

DIMACSAddNode::DIMACSAddNode(const FlowGraphNode& node,
                             const vector<FlowGraphArc*>& arcs) :
     DIMACSChange(), id_(node.id_), excess_(node.excess_), type_(node.type_) {
  for (FlowGraphArc* arc : arcs) {
    arc_additions_.push_back(DIMACSNewArc(*arc));
    stats_.arcs_added_++;
  }
  stats_.nodes_added_++;
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
