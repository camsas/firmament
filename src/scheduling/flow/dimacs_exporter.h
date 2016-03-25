// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Export utility that converts a given resource topology and set of job's into
// a DIMACS file for use with the solvers.

#ifndef FIRMAMENT_SCHEDULING_FLOW_DIMACS_EXPORTER_H
#define FIRMAMENT_SCHEDULING_FLOW_DIMACS_EXPORTER_H

#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

class DIMACSExporter {
 public:
  DIMACSExporter();
  void Export(const FlowGraph& graph, FILE* stream);
  void ExportIncremental(const vector<DIMACSChange*>& changes, FILE* stream);

 private:
  inline void GenerateArc(const FlowGraphArc& arc, FILE* stream);
  inline void GenerateNode(const FlowGraphNode& node, FILE* stream);
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_DIMACS_EXPORTER_H
