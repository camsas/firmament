// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_new_arc.h"

namespace firmament {

  const string DIMACSNewArc::GenerateChange() const {
    stringstream ss;
    ss << "a " << src_ << " " << dst_ << " " << cap_lower_bound_
       << " " << cap_upper_bound_ << " " << cost_ << "\n";
    return ss.str();
  }

} // namespace firmament
