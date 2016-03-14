// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H

#include <string>

#include "base/types.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/flow_graph_arc.h"

namespace firmament {

class DIMACSNewArc : public DIMACSChange {
 public:
  explicit DIMACSNewArc(const FlowGraphArc& arc);
  const string GenerateChange() const;

  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  uint64_t cost_;
  FlowGraphArcType type_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_NEW_ARC_H
