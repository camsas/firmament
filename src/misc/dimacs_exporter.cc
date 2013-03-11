// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of export utility that converts a given resource topology and
// set of job's into a DIMACS file for use with the Quincy CS2 solver.


#include <string>

#include <cstdio>

#include <boost/bind.hpp>

#include "misc/dimacs_exporter.h"
#include "misc/pb_utils.h"

namespace firmament {

using machine::topology::TopologyManager;

DIMACSExporter::DIMACSExporter()
    : output_("") {
}

void DIMACSExporter::Export(const FlowGraph& graph) {
  // Problem header
  output_ += GenerateHeader(0, 0);
  // ----------------------------
  // Supply nodes
  // ----------------------------
  // Task nodes
  output_ += GenerateComment("Task nodes");
  // Unscheduled aggregator nodes
  output_ += GenerateComment("Unscheduled agg nodes");
  // Resource nodes (implicit)
  output_ += GenerateComment("(Other nodes are implicit)");

  // ----------------------------
  // Demand nodes
  // ----------------------------
  // Sink node
  output_ += GenerateComment("Sink node");

  // ----------------------------
  // Arcs
  // ----------------------------
  // Task -> resource prefs
  output_ += GenerateComment("Task preference arcs");
  // Unscheduled aggregator nodes -> sink
  output_ += GenerateComment("Unscheduled agg nodes -> sink");
  // Cluster aggregator -> res topology
  output_ += GenerateComment("Cluster agg node (X) -> sink");
  // Resource topology
  output_ += GenerateComment("Resource topology (internal arcs)");
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
  int64_t value = node.demand_ - node.supply_;
  stringstream ss;
  ss << "n " << node.id_ << " " << value << "\n";
  return ss.str();
}

}  // namespace firmament
