// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_remove_node.h"

namespace firmament {

  const string DIMACSRemoveNode::GenerateChange() const {
    stringstream ss;
    ss << "r " << node_.id_;
    return ss.str();
  }

} // namespace firmament
