// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a DIMACS file for use with the Quincy CS2 solver.

#ifndef FIRMAMENT_MISC_FLOW_GRAPH_NODE_H
#define FIRMAMENT_MISC_FLOW_GRAPH_NODE_H

#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "base/task_desc.pb.h"
#include "scheduling/flow_node_type.pb.h"

namespace firmament {

struct FlowGraphNode {
  explicit FlowGraphNode(uint64_t id)
      : id_(id), supply_(0), demand_(0) {}
  FlowGraphNode(uint64_t id, uint64_t supply, uint64_t demand)
      : id_(id), supply_(supply), demand_(demand) {
  }

  uint64_t id_;
  uint64_t supply_;
  uint64_t demand_;
  FlowNodeType type_;
  // TODO(malte): Not sure if these should be here, but they've got to go
  // somewhere.
  ResourceID_t resource_id_;
  TaskID_t task_id_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_FLOW_GRAPH_NODE_H
