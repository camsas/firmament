/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */


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
