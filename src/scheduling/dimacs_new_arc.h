// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_NEW_ARC_H
#define FIRMAMENT_SCHEDULING_DIMACS_NEW_ARC_H

#include <string>

#include "base/types.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/flow_graph_arc.h"

namespace firmament {

class DIMACSNewArc : public DIMACSChange {
 public:
  explicit DIMACSNewArc(const FlowGraphArc& arc): DIMACSChange(),
    src_(arc.src_), dst_(arc.dst_), cap_lower_bound_(arc.cap_lower_bound_),
    cap_upper_bound_(arc.cap_upper_bound_), cost_(arc.cost_) {
  }

  const string GenerateChange() const;

 private:
  FRIEND_TEST(FlowGraphTest, AddResourceNode);
  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  uint64_t cost_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_NEW_ARC_H
