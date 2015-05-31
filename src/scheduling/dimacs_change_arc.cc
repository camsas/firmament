// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <string>

#include "scheduling/dimacs_change_arc.h"

namespace firmament {

DIMACSChangeArc::DIMACSChangeArc(const FlowGraphArc& arc) : DIMACSChange(),
  src_(arc.src_), dst_(arc.dst_), cap_lower_bound_(arc.cap_lower_bound_),
  cap_upper_bound_(arc.cap_upper_bound_), cost_(arc.cost_) {
  // An upper bound capacity of 0 indicates arc removal
  if (cap_upper_bound_ == 0)
    stats_.arcs_removed_++;
  else
    stats_.arcs_changed_++;
}

const string DIMACSChangeArc::GenerateChange() const {
  stringstream ss;
  ss << DIMACSChange::GenerateChangeDescription();
  ss << "x " << src_ << " " << dst_ << " " << cap_lower_bound_
     << " " << cap_upper_bound_ << " " << cost_ << "\n";
  return ss.str();
}

} // namespace firmament
