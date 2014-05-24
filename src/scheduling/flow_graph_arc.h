// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Represents an arc in the scheduling flow graph.

#ifndef FIRMAMENT_MISC_FLOW_GRAPH_ARC_H
#define FIRMAMENT_MISC_FLOW_GRAPH_ARC_H

#include <string>

#include "base/common.h"
#include "base/types.h"

namespace firmament {

struct FlowGraphArc {
  FlowGraphArc(uint64_t src, uint64_t dst)
      : src_(src), dst_(dst), cap_lower_bound_(0),
        cap_upper_bound_(0), cost_(0) {}
  FlowGraphArc(uint64_t src, uint64_t dst, uint64_t clb, uint64_t cub,
               uint64_t cost)
      : src_(src), dst_(dst), cap_lower_bound_(clb), cap_upper_bound_(cub),
        cost_(cost) {
  }

  uint64_t src_;
  uint64_t dst_;
  uint64_t cap_lower_bound_;
  uint64_t cap_upper_bound_;
  uint64_t cost_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_FLOW_GRAPH_ARC_H
