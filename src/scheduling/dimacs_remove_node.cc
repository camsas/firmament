// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_remove_node.h"

namespace firmament {

  const string DIMACSRemoveNode::GenerateChange() const {
    stringstream ss;
    ss << DIMACSChange::GenerateChange();
    ss << "r " << node_id_ << "\n";
    return ss.str();
  }

} // namespace firmament
