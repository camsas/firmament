// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a DIMACS file for use with the Quincy CS2 solver.

#ifndef FIRMAMENT_MISC_FLOW_GRAPH_NODE_H
#define FIRMAMENT_MISC_FLOW_GRAPH_NODE_H

#include <string>

#include <boost/uuid/nil_generator.hpp>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "base/task_desc.pb.h"
#include "scheduling/flow_node_type.pb.h"

namespace firmament {

struct FlowGraphNode {
  explicit FlowGraphNode(uint64_t id)
      : id_(id), excess_(0), resource_id_(boost::uuids::nil_uuid()),
    task_id_(0) {}
  FlowGraphNode(uint64_t id, uint64_t excess)
      : id_(id), excess_(excess), resource_id_(boost::uuids::nil_uuid()),
    task_id_(0) {
  }
  void AddArc(FlowGraphArc* arc) {
    CHECK_EQ(arc->src_, id_);
    InsertIfNotPresent(&outgoing_arc_map_, arc->dst_, arc);
  }

  uint64_t id_;
  int64_t excess_;
  FlowNodeType type_;
  // TODO(malte): Not sure if these should be here, but they've got to go
  // somewhere.
  ResourceID_t resource_id_;
  // The ID of the task represented by this node.
  TaskID_t task_id_;
  // The ID of the job that this task belongs to.
  JobID_t job_id_;
  // Free-form comment for debugging purposes (used to label special nodes)
  string comment_;
  // Outgoing arcs from this node, keyed by destination node
  unordered_map<uint64_t, FlowGraphArc*> outgoing_arc_map_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_FLOW_GRAPH_NODE_H
