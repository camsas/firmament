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

DIMACSExporter::DIMACSExporter()
    : output_("") {
}

void DIMACSExporter::Export(const FlowGraph& graph) {
  // Problem header
  output_ += GenerateHeader(graph.NumNodes(), graph.NumArcs());
  // ----------------------------
  // Supply nodes
  // ----------------------------
  // Task nodes
  //output_ += GenerateComment("Task nodes");
  // for ...
  // output_ += GenerateNode();
  // Unscheduled aggregator nodes
  //output_ += GenerateComment("Unscheduled agg nodes");
  // for ...
  // output_ += GenerateNode();
  // Resource nodes (implicit)
  //output_ += GenerateComment("(Other nodes are implicit)");
  // XXX(malte): above comments are meaningless; currently all nodes are just
  // dumped in one go.
  output_ += GenerateComment("=== ALL NODES FOLLOW ===");
  for (unordered_map<uint64_t, FlowGraphNode*>::const_iterator n_iter =
       graph.Nodes().begin();
       n_iter != graph.Nodes().end();
       ++n_iter)
    output_ += GenerateNode(*n_iter->second);

  // ----------------------------
  // Demand nodes
  // ----------------------------
  // Sink node
  //output_ += GenerateComment("Sink node");
  //output_ += GenerateNode(graph.sink_node());
  // ----------------------------
  // Arcs
  // ----------------------------
  // Task -> resource prefs
  //output_ += GenerateComment("Task preference arcs");
  // Unscheduled aggregator nodes -> sink
  //output_ += GenerateComment("Unscheduled agg nodes -> sink");
  // Cluster aggregator -> res topology
  //output_ += GenerateComment("Cluster agg node (X) -> resource topo");
  // Resource topology
  //output_ += GenerateComment("Resource topology (internal arcs)");
  // XXX(malte): above comments are meaningless; currently all arcs are just
  // dumped in one go.
  output_ += GenerateComment("=== ALL ARCS FOLLOW ===");
  for (unordered_set<FlowGraphArc*>::const_iterator a_iter =
       graph.Arcs().begin();
       a_iter != graph.Arcs().end();
       ++a_iter)
    output_ += GenerateArc(**a_iter);
  // Add end of iteration comment.
  output_ += GenerateComment("EOI");
}

void DIMACSExporter::ExportIncremental(const vector<DIMACSChange*>& changes) {
  for (vector<DIMACSChange*>::const_iterator it = changes.begin();
       it != changes.end(); ++it) {
    output_ += (*it)->GenerateChange();
  }
  // Add end of iteration comment.
  output_ += GenerateComment("EOI");
}

void DIMACSExporter::FlushAndClose(const string& filename) {
  // Write the cached DIMACS graph string out to the file
  FILE* outfd = fopen(filename.c_str(), "w");
  CHECK(outfd != NULL) << "Failed to open file " << filename
                       << " to communicate with the solver";
  fprintf(outfd, "%s", output_.c_str());
  CHECK_EQ(fclose(outfd), 0);
}

void DIMACSExporter::FlushAndClose(int fd) {
  // Write the cached DIMACS graph string out to the file
  FILE *stream = fdopen(fd, "w");
  CHECK(stream != NULL) << "Failed to open FD to solver for writing. FD: "
                        << fd;
  fprintf(stream, "%s", output_.c_str());
  if (fflush(stream)) {
    LOG(FATAL) << "Error while flushing";
  }
  CHECK_EQ(fclose(stream), 0);
}

void DIMACSExporter::Flush(FILE* stream) {
  int result = fputs(output_.c_str(), stream);
  if (result < 0) {
    PLOG(FATAL) << "Failed to write " << output_.length()
                << " bytes to solver.";
  }
  if (fflush(stream)) {
    PLOG(FATAL) << "Error while flushing";
  }
}

const string DIMACSExporter::GenerateArc(const FlowGraphArc& arc) {
  stringstream ss;
  ss << "a " << arc.src_ << " " << arc.dst_ << " " << arc.cap_lower_bound_
     << " " << arc.cap_upper_bound_ << " " << arc.cost_ << "\n";
  return ss.str();
}

const string DIMACSExporter::GenerateComment(const string& text) {
  stringstream ss;
  ss << "c " << text << "\n";
  return ss.str();
}

const string DIMACSExporter::GenerateHeader(uint64_t num_nodes,
                                            uint64_t num_arcs) {
  stringstream ss;
  ss << "c ===========================\n";
  ss << "p min " << num_nodes << " " << num_arcs << "\n";
  ss << "c ===========================\n";
  return ss.str();
}

const string DIMACSExporter::GenerateNode(const FlowGraphNode& node) {
  stringstream ss;
  if (node.comment_ != "") {
    ss << "c nd " << node.task_id_ << " " << node.comment_ << "\n";
  } else if (!node.resource_id_.is_nil()) {
    ss << "c nd " << node.task_id_ << " " << to_string(node.resource_id_)
       << "\n";
  } else if (node.task_id_) {
    ss << "c nd T_" << node.task_id_ << "\n";
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
  ss << "n " << node.id_ << " " << node.excess_ << " " << node_type << "\n";
  return ss.str();
}

}  // namespace firmament
