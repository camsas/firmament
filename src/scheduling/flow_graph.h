// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#ifndef FIRMAMENT_MISC_FLOW_GRAPH_H
#define FIRMAMENT_MISC_FLOW_GRAPH_H

#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "engine/topology_manager.h"
#include "misc/map-util.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"

namespace firmament {

class FlowGraph {
 public:
  FlowGraph();
  void AddJobNodes(JobDescriptor* jd);
  void AddResourceNode(ResourceTopologyNodeDescriptor* rtnd,
                       uint64_t* leaf_counter);
  void AddResourceTopology(ResourceTopologyNodeDescriptor* resource_tree);
  void AddTaskNode();
  void DeleteTaskNode(FlowGraphNode* node);
  FlowGraphNode* NodeForResourceID(const ResourceID_t& res_id);
  FlowGraphNode* NodeForTaskID(TaskID_t task_id);
  void UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id);
  inline const unordered_set<FlowGraphArc*>& Arcs() const { return arc_set_; }
  inline const unordered_map<uint64_t, FlowGraphNode*>& Nodes() const {
    return node_map_;
  }
  inline const unordered_set<uint64_t>& leaf_node_ids() const {
    return leaf_nodes_;
  }
  inline const unordered_set<uint64_t>& task_node_ids() const {
    return task_nodes_;
  }
  inline const FlowGraphNode& sink_node() const { return *sink_node_; }
  inline const FlowGraphNode& cluster_agg_node() const {
    return *cluster_agg_node_;
  }
  inline uint64_t NumArcs() const { return arc_set_.size(); }
  inline uint64_t NumNodes() const { return node_map_.size(); }
  inline FlowGraphNode* Node(uint64_t id) {
    FlowGraphNode* const* npp = FindOrNull(node_map_, id);
    return (npp ? *npp : NULL);
  }

 protected:
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(FlowGraphTest, AddArcToNode);
  void AddArcsForTask(TaskDescriptor* cur, FlowGraphNode* task_node,
                      FlowGraphNode* unsched_agg_node,
                      FlowGraphArc* unsched_agg_to_sink_arc);
  FlowGraphArc* AddArcInternal(FlowGraphNode* src, FlowGraphNode* dst);
  FlowGraphNode* AddNodeInternal(uint64_t id);
  FlowGraphArc* AddArcInternal(uint64_t src, uint64_t dst);
  void AddSpecialNodes();
  void DeleteArc(FlowGraphArc* arc);
  void PinTaskToNode(FlowGraphNode* task_node, FlowGraphNode* res_node);

  inline uint64_t next_id() { return current_id_++; }

  // Graph structure containers and helper fields
  uint64_t current_id_;
  unordered_map<uint64_t, FlowGraphNode*> node_map_;
  unordered_set<FlowGraphArc*> arc_set_;
  FlowGraphNode* cluster_agg_node_;
  FlowGraphArc* cluster_agg_into_res_topo_arc_;
  FlowGraphNode* sink_node_;
  // Resource and task mappings
  unordered_map<TaskID_t, uint64_t> task_to_nodeid_map_;
  unordered_map<ResourceID_t, uint64_t,
      boost::hash<boost::uuids::uuid> > resource_to_nodeid_map_;
  // The "node ID" for the job is currently the ID of the job's unscheduled node
  unordered_map<JobID_t, uint64_t,
      boost::hash<boost::uuids::uuid> > job_to_nodeid_map_;
  unordered_set<uint64_t> leaf_nodes_;
  unordered_set<uint64_t> task_nodes_;
};

}  // namespace firmament

#endif  // FIRMAMENT_MISC_FLOW_GRAPH_H
