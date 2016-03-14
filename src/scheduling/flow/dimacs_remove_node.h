// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_REMOVE_NODE_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_REMOVE_NODE_H

#include <string>

#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

class DIMACSRemoveNode : public DIMACSChange {
 public:
  explicit DIMACSRemoveNode(const FlowGraphNode& node);
  const string GenerateChange() const;

  const uint64_t node_id_;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_FLOW_DIMACS_REMOVE_NODE_H
