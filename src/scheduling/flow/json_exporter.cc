// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of export utility that converts a given resource topology and
// a JSON object for use e.g. with viz.js.

#include "scheduling/flow/json_exporter.h"

#include <string>
#include <cstdio>

#include "misc/pb_utils.h"

namespace firmament {

using machine::topology::TopologyManager;

JSONExporter::JSONExporter() {
}

void JSONExporter::Export(const FlowGraph& graph, string* output) const {
  // Problem header
  *output += GenerateHeader(graph.NumNodes(), graph.NumArcs());
  *output += "\"nodes\": [";
  for (unordered_map<uint64_t, FlowGraphNode*>::const_iterator n_iter =
       graph.Nodes().begin();
       n_iter != graph.Nodes().end();
       ++n_iter) {
    if (n_iter != graph.Nodes().begin())
      *output += ",\n";
    *output += GenerateNode(*n_iter->second);
  }
  *output += "],\n";

  *output += "\"edges\": [";
  for (unordered_set<FlowGraphArc*>::const_iterator a_iter =
       graph.Arcs().begin();
       a_iter != graph.Arcs().end();
       ++a_iter) {
    if (a_iter != graph.Arcs().begin())
      *output += ",\n";
    *output += GenerateArc(**a_iter);
  }
  *output += "]\n";
  *output += GenerateFooter();
}

const string JSONExporter::GenerateArc(const FlowGraphArc& arc) const {
  stringstream ss;
  ss << "{ \"from\": " << arc.src_ << ", \"to\": " << arc.dst_
     << ", \"label\": \"cap: " << arc.cap_lower_bound_ << "/"
     << arc.cap_upper_bound_ << ", c: " << arc.cost_ << "\" }";
  return ss.str();
}

const string JSONExporter::GenerateComment(const string& text) const {
  stringstream ss;
  ss << "/* " << text << " */";
  return ss.str();
}

const string JSONExporter::GenerateFooter() const {
  stringstream ss;
  ss << " }";
  return ss.str();
}

const string JSONExporter::GenerateHeader(uint64_t num_nodes,
                                          uint64_t num_arcs) const {
  stringstream ss;
  ss << "{ ";
  return ss.str();
}

const string JSONExporter::GenerateNode(const FlowGraphNode& node) const {
  stringstream ss;
  stringstream label;
  string node_shape;
  string node_color;
  if (node.type_ == FlowNodeType::SINK) {
    node_shape = "diamond";
    node_color = "#cccccc";
  } else {
    node_shape = "box";
    if (node.type_ == FlowNodeType::UNSCHEDULED_TASK) {
      node_color = "#ff0000";
    } else if (node.type_ == FlowNodeType::SCHEDULED_TASK) {
      node_color = "#00ff00";
    } else {
      node_color = "#ffffff";
    }
  }
  if (node.comment_ != "") {
    label << node.comment_;
  } else if (!node.resource_id_.is_nil()) {
    label << "res: " << to_string(node.resource_id_);
  } else if (node.task_id_) {
    label << "tsk: " << node.task_id_;
  }
  ss << "{ \"id\": " << node.id_ << ", \"label\": \"" << label.str() << "\", "
     << "\"shape\": \"" << node_shape << "\", "
     << "\"color\": { \"background\": \"" << node_color << "\" } }";
  return ss.str();
}

}  // namespace firmament
