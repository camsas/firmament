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

// Implementation of export utility that converts a given resource topology and
// set of job's into a DIMACS file for use with the Quincy CS2 solver.

#include "scheduling/flow/dimacs_exporter.h"

#include <string>
#include <cstdio>
#include <boost/bind.hpp>

#include "misc/pb_utils.h"

namespace firmament {

DIMACSExporter::DIMACSExporter() {
}

void DIMACSExporter::Export(const FlowGraph& graph, FILE* stream) {
  fprintf(stream, "c ===========================\n");
  fflush(stream);
  fprintf(stream, "p min %" PRIu64 " %" PRIu64 "\n",
          graph.NumNodes(), graph.NumArcs());
  fflush(stream);
  fprintf(stream, "c ===========================\n");
  fflush(stream);
  fprintf(stream, "c === ALL NODES FOLLOW ===\n");
  fflush(stream);
  for (auto& id_node : graph.Nodes()) {
    GenerateNode(*id_node.second, stream);
  }
  fprintf(stream, "c === ALL ARCS FOLLOW ===\n");
  fflush(stream);
  for (const auto& arc : graph.Arcs()) {
    GenerateArc(*arc, stream);
  }
  // Add end of iteration comment.
  fprintf(stream, "c EOI\n");
  fflush(stream);
}

void DIMACSExporter::ExportIncremental(const vector<DIMACSChange*>& changes,
                                       FILE* stream) {
  for (const auto& change : changes) {
    fprintf(stream, "%s", change->GenerateChange().c_str());
    fflush(stream);
  }
  // Add end of iteration comment.
  fprintf(stream, "c EOI\n");
  fflush(stream);
}

inline void DIMACSExporter::GenerateArc(const FlowGraphArc& arc, FILE* stream) {
  fprintf(stream,
          "a %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRId64 "\n",
          arc.src_, arc.dst_, arc.cap_lower_bound_, arc.cap_upper_bound_,
          arc.cost_);
  fflush(stream);
}

inline void DIMACSExporter::GenerateNode(const FlowGraphNode& node,
                                         FILE* stream) {
  if (node.rd_ptr_) {
    fprintf(stream, "c nd Res_%s\n", node.rd_ptr_->uuid().c_str());
  } else if (node.td_ptr_) {
    fprintf(stream, "c nd Task_%" PRIu64 "\n", node.td_ptr_->uid());
  } else if (node.ec_id_) {
    fprintf(stream, "c nd EC_%" PRIu64 "\n", node.ec_id_);
  } else if (node.comment_ != "") {
    fprintf(stream, "c nd %s\n", node.comment_.c_str());
  }
  uint32_t node_type = 0;
  if (node.type_ == FlowNodeType::PU) {
    node_type = 2;
  } else if (node.type_ == FlowNodeType::MACHINE) {
    node_type = 4;
  } else if (node.type_ == FlowNodeType::NUMA_NODE ||
             node.type_ == FlowNodeType::SOCKET ||
             node.type_ == FlowNodeType::CACHE ||
             node.type_ == FlowNodeType::CORE) {
    node_type = 5;
  } else if (node.type_ == FlowNodeType::SINK) {
    node_type = 3;
  } else if (node.type_ == FlowNodeType::UNSCHEDULED_TASK ||
             node.type_ == FlowNodeType::SCHEDULED_TASK ||
             node.type_ == FlowNodeType::ROOT_TASK) {
    node_type = 1;
  } else {
    node_type = 0;
  }
  fprintf(stream, "n %" PRIu64 " %" PRId64 " %d\n",
          node.id_, node.excess_, node_type);
  fflush(stream);
}

}  // namespace firmament
