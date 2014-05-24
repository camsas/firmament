// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of export utility that converts a given resource topology and
// set of job's into a DIMACS file for use with the Quincy CS2 solver.


#include <string>

#include <cstdio>

#include <boost/bind.hpp>

#include "misc/pb_utils.h"
#include "scheduling/dimacs_exporter.h"

namespace firmament {

using machine::topology::TopologyManager;

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
}

void DIMACSExporter::Flush(const string& filename) {
  // TODO(malte): Sanity checks
  // Write the cached DIMACS graph string out to the file
  FILE* outfd = fopen(filename.c_str(), "w");
  fprintf(outfd, "%s", output_.c_str());
  fclose(outfd);
}

void DIMACSExporter::Flush(int fd) {
  // TODO(malte): Sanity checks
  // Write the cached DIMACS graph string out to the file
  FILE *stream;
  if ((stream = fdopen(fd, "w")) == NULL) {
    LOG(ERROR) << "Failed to open FD to solver for writing. FD: " << fd;
  } else {
    fprintf(stream, "%s", output_.c_str());
    fflush(stream);
  }
  fclose(stream);
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
  if (node.supply_ > 0)
    CHECK_EQ(node.demand_, 0);
  if (node.demand_ > 0)
    CHECK_EQ(node.supply_, 0);
  int64_t value = node.supply_ - node.demand_;
  stringstream ss;
  if (!node.resource_id_.is_nil())
    ss << "c nd " << node.task_id_ << " " << to_string(node.resource_id_)
       << "\n";
  else if (node.comment_ != "")
    ss << "c nd " << node.task_id_ << " " << node.comment_ << "\n";
  ss << "n " << node.id_ << " " << value << "\n";
  return ss.str();
}

}  // namespace firmament
