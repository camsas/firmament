// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <string>

#include "scheduling/flow/dimacs_new_arc.h"

namespace firmament {

DIMACSNewArc::DIMACSNewArc(const FlowGraphArc& arc) : DIMACSChange(),
  src_(arc.src_), dst_(arc.dst_), cap_lower_bound_(arc.cap_lower_bound_),
  cap_upper_bound_(arc.cap_upper_bound_), cost_(arc.cost_) {
  stats_.arcs_added_++;
}

const string DIMACSNewArc::GenerateChange() const {
  stringstream ss;
  ss << DIMACSChange::GenerateChangeDescription();
  ss << "a " << src_ << " " << dst_ << " " << cap_lower_bound_
     << " " << cap_upper_bound_ << " " << cost_ << "\n";
  return ss.str();
}

} // namespace firmament
