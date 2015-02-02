// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_add_node.h"

namespace firmament {

  const string DIMACSAddNode::GenerateChange() const {
    stringstream ss;
    // TODO(ionel): add support for node price. Currently we just print 0.
    ss << "d " << node_.id_ << " " << node_.excess_ << " 0 " << arcs_->size();
    for (vector<FlowGraphArc*>::const_iterator it = arcs_->begin(); it != arcs_->end(); ++it) {
      ss << "a " << (*it)->src_ << " " << (*it)->dst_ << " " << (*it)->cap_lower_bound_ << " "
         << (*it)->cap_upper_bound_ << " " << (*it)->cost_ << "\n";
    }
    return ss.str();
  }

} // namespace firmament
