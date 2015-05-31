// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H
#define FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H

#include <string>

#include "base/types.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/dimacs_change_stats.h"
#include "scheduling/flow_graph_arc.h"

namespace firmament {

class DIMACSChangeArc : public DIMACSChange {
 public:
  explicit DIMACSChangeArc(const FlowGraphArc& arc);

  const string GenerateChange() const;

 protected:
  uint64_t upper_bound() {
    return cap_upper_bound_;
  }

 private:
  FRIEND_TEST(FlowGraphTest, AddResourceNode);
  FRIEND_TEST(FlowGraphTest, AddOrUpdateJobNodes);
  FRIEND_TEST(FlowGraphTest, ChangeArc);
  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  uint64_t cost_;

  friend DIMACSChangeStats;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_CHANGE_ARC_H
