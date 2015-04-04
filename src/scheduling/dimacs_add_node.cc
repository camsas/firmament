// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <string>
#include <vector>

#include "scheduling/dimacs_add_node.h"

namespace firmament {

  const string DIMACSAddNode::GenerateChange() const {
    stringstream ss;
    ss << DIMACSChange::GenerateChange();
    ss << "n " << node_.id_ << " " << node_.excess_ << " " << GetNodeType()
       << "\n";
    for (vector<FlowGraphArc*>::const_iterator it = arcs_->begin();
         it != arcs_->end(); ++it) {
      ss << "a " << (*it)->src_ << " " << (*it)->dst_ << " "
         << (*it)->cap_lower_bound_ << " " << (*it)->cap_upper_bound_
         << " " << (*it)->cost_ << "\n";
    }
    return ss.str();
  }

  uint32_t DIMACSAddNode::GetNodeType() const {
    if (node_.type_.type() == FlowNodeType::PU) {
      return 2;
    } else if (node_.type_.type() == FlowNodeType::SINK) {
      return 3;
    } else if (node_.type_.type() == FlowNodeType::UNSCHEDULED_TASK ||
               node_.type_.type() == FlowNodeType::SCHEDULED_TASK ||
               node_.type_.type() == FlowNodeType::ROOT_TASK) {
      return 1;
    } else {
      return 0;
    }
  }

} // namespace firmament
