// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include <string>

#include "scheduling/flow/dimacs_change_arc.h"

namespace firmament {

DIMACSChangeArc::DIMACSChangeArc(const FlowGraphArc& arc,
                                 const uint64_t old_cost)
  : DIMACSChange(), src_(arc.src_), dst_(arc.dst_),
    cap_lower_bound_(arc.cap_lower_bound_),
    cap_upper_bound_(arc.cap_upper_bound_), cost_(arc.cost_), type_(arc.type_),
    old_cost_(old_cost) {
}

const string DIMACSChangeArc::GenerateChange() const {
  stringstream ss;
  ss << DIMACSChange::GenerateChangeDescription();
  ss << "x " << src_ << " " << dst_ << " " << cap_lower_bound_ << " "
     << cap_upper_bound_ << " " << cost_ << " " << type_ << " " << old_cost_
     << "\n";
  return ss.str();
}

} // namespace firmament
