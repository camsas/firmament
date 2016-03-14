// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H

#include <string>
#include <vector>

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

class DIMACSAddNode : public DIMACSChange {
 public:
  DIMACSAddNode(const FlowGraphNode& node, const vector<FlowGraphArc*>& arcs);
  ~DIMACSAddNode() {}
  const string GenerateChange() const;
  uint32_t GetNodeType() const;
  const uint64_t id_;
  const uint64_t excess_;
  const FlowNodeType type_;
  vector<DIMACSNewArc> arc_additions_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_ADD_NODE_H
