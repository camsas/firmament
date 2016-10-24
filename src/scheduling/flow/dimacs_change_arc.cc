/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#include <string>

#include "scheduling/flow/dimacs_change_arc.h"

namespace firmament {

DIMACSChangeArc::DIMACSChangeArc(const FlowGraphArc& arc,
                                 const int64_t old_cost)
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
