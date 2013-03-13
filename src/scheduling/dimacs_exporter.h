// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a DIMACS file for use with the Quincy CS2 solver.

#ifndef FIRMAMENT_MISC_DIMACS_EXPORTER_H
#define FIRMAMENT_MISC_DIMACS_EXPORTER_H

#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "engine/topology_manager.h"
#include "scheduling/flow_graph.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"

namespace firmament {

class DIMACSExporter {
 public:
  DIMACSExporter();
  void Export(const FlowGraph& graph);
  void Flush(const string& filename);
  void Reset() { output_ = ""; }

 private:
  const string GenerateArc(const FlowGraphArc& arc);
  const string GenerateComment(const string& text);
  const string GenerateHeader(uint64_t num_nodes,
                              uint64_t num_arcs);
  const string GenerateNode(const FlowGraphNode& node);
  const string GenerateTaskNode();
  void GenerateResourceNode(ResourceDescriptor* rd,
                            string* output);

  string output_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_DIMACS_EXPORTER_H
