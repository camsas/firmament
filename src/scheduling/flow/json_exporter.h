// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a JSON object for use e.g. with viz.js.

#ifndef FIRMAMENT_SCHEDULING_FLOW_JSON_EXPORTER_H
#define FIRMAMENT_SCHEDULING_FLOW_JSON_EXPORTER_H

#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

class JSONExporter {
 public:
  JSONExporter();
  void Export(const FlowGraph& graph, string* output) const;

 private:
  const string GenerateArc(const FlowGraphArc& arc) const;
  const string GenerateComment(const string& text) const;
  const string GenerateFooter() const;
  const string GenerateHeader(uint64_t num_nodes, uint64_t num_arcs) const;
  const string GenerateNode(const FlowGraphNode& node) const;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_JSON_EXPORTER_H
