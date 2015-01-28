// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/dimacs_change_arc.h"

namespace firmament {

  const string DIMACSChangeArc::GenerateChange() const {
    stringstream ss;
    ss << "x " << arc_.src_ << " " << arc_.dst_ << " " << arc_.cap_lower_bound_
       << " " << arc_.cap_upper_bound_ << " " << arc_.cost_ << "\n";
    return ss.str();
  }

} // namespace firmament
