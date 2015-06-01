// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a DIMACS file for use with the Quincy CS2 solver.

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_NODE_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_NODE_H

#include <string>
#include <boost/uuid/nil_generator.hpp>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "base/task_desc.pb.h"
#include "scheduling/flow/flow_graph_arc.h"

namespace firmament {

enum FlowNodeType {
  ROOT_TASK = 0,
  SCHEDULED_TASK = 1,
  UNSCHEDULED_TASK = 2,
  GLOBAL_AGGREGATOR = 3,
  JOB_AGGREGATOR = 4,
  RACK_AGGREGATOR = 5,  // currently unused
  MACHINE = 6,
  PU = 7,
  SINK = 8,
  UNKNOWN = 9,
  EQUIVALENCE_CLASS = 10,
};

struct FlowGraphNode {
  explicit FlowGraphNode(uint64_t id);
  FlowGraphNode(uint64_t id, uint64_t excess);
  void AddArc(FlowGraphArc* arc);

  uint64_t id_;
  int64_t excess_;
  FlowNodeType type_;
  // TODO(malte): Not sure if these should be here, but they've got to go
  // somewhere.
  // The ID of the job that this task belongs to (if task node).
  JobID_t job_id_;
  // The ID of the resource that this node represents.
  ResourceID_t resource_id_;
  // The ID of the task represented by this node.
  TaskID_t task_id_;
  // Free-form comment for debugging purposes (used to label special nodes)
  string comment_;
  // Outgoing arcs from this node, keyed by destination node
  unordered_map<uint64_t, FlowGraphArc*> outgoing_arc_map_;
  // Incoming arcs to this node, keyed by source node
  unordered_map<uint64_t, FlowGraphArc*> incoming_arc_map_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_NODE_H
