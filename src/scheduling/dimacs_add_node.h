// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H
#define FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H

#include <string>
#include <vector>

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/dimacs_new_arc.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"

namespace firmament {

class DIMACSAddNode : public DIMACSChange {
 public:
  DIMACSAddNode(const FlowGraphNode& node, const vector<FlowGraphArc*>& arcs);
  ~DIMACSAddNode() {}

  const string GenerateChange() const;

 private:
  uint32_t GetNodeType() const;
  FRIEND_TEST(FlowGraphTest, AddResourceNode);
  FRIEND_TEST(FlowGraphTest, AddOrUpdateJobNodes);
  const uint64_t id_, excess_;
  const FlowNodeType type_;
  vector<DIMACSNewArc> arc_additions_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H
