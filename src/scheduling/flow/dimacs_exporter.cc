// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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
  fprintf(stream, "p min %" PRIu64 " %" PRIu64 "\n",
          graph.NumNodes(), graph.NumArcs());
  fprintf(stream, "c ===========================\n");
  fprintf(stream, "c === ALL NODES FOLLOW ===\n");
  for (auto& id_node : graph.Nodes()) {
    GenerateNode(*id_node.second, stream);
  }
  fprintf(stream, "c === ALL ARCS FOLLOW ===\n");
  for (const auto& arc : graph.Arcs()) {
    GenerateArc(*arc, stream);
  }
  // Add end of iteration comment.
  fprintf(stream, "c EOI\n");
}

void DIMACSExporter::ExportIncremental(const vector<DIMACSChange*>& changes,
                                       FILE* stream) {
  for (const auto& change : changes) {
    fprintf(stream, "%s", change->GenerateChange().c_str());
  }
  // Add end of iteration comment.
  fprintf(stream, "c EOI\n");
}

inline void DIMACSExporter::GenerateArc(const FlowGraphArc& arc, FILE* stream) {
  fprintf(stream,
          "a %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 " %" PRIu64 "\n",
          arc.src_, arc.dst_, arc.cap_lower_bound_, arc.cap_upper_bound_,
          arc.cost_);
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
}

}  // namespace firmament
