// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_remove_node.h"

namespace firmament {

DIMACSRemoveNode::DIMACSRemoveNode(const FlowGraphNode& node) :
  DIMACSChange(),
  node_id_(node.id_) {
  stats_.nodes_removed_++;
}

const string DIMACSRemoveNode::GenerateChange() const {
  stringstream ss;
  ss << DIMACSChange::GenerateChangeDescription();
  ss << "r " << node_id_ << "\n";
  return ss.str();
}

} // namespace firmament
