// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H
#define FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H

#include <string>
#include <vector>

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"

namespace firmament {

class DIMACSAddNode : public DIMACSChange {
 public:
  DIMACSAddNode(const FlowGraphNode& node, vector<FlowGraphArc*>* arcs):
    DIMACSChange(), node_(node), arcs_(arcs) {
  }

  ~DIMACSAddNode() {
    delete arcs_;
  }

  const string GenerateChange() const;

 private:
  FRIEND_TEST(FlowGraphTest, AddResourceNode);
  FRIEND_TEST(FlowGraphTest, AddOrUpdateJobNodes);
  const FlowGraphNode& node_;
  vector<FlowGraphArc*>* arcs_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DIMACS_ADD_NODE_H
