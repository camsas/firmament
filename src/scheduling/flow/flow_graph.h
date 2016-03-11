// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_H

#include <queue>
#include <vector>

#include "misc/map-util.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

DECLARE_string(flow_scheduling_solver);

namespace firmament {

class FlowGraph {
 public:
  FlowGraph();
  ~FlowGraph();
  FlowGraphArc* AddArc(FlowGraphNode* src, FlowGraphNode* dst);
  FlowGraphArc* AddArc(uint64_t src, uint64_t dst);
  FlowGraphNode* AddNode();
  void ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                 uint64_t cap_upper_bound, uint64_t cost);
  void ChangeArcCost(FlowGraphArc* arc, uint64_t cost);
  void DeleteArc(FlowGraphArc* arc);
  void DeleteNode(FlowGraphNode* node);
  FlowGraphArc* GetArc(FlowGraphNode* src, FlowGraphNode* dst);
  inline const unordered_set<FlowGraphArc*>& Arcs() const { return arc_set_; }
  inline const unordered_map<uint64_t, FlowGraphNode*>& Nodes() const {
    return node_map_;
  }
  inline const FlowGraphNode& Node(uint64_t id) const {
    FlowGraphNode* node = FindPtrOrNull(node_map_, id);
    CHECK_NOTNULL(node);
    return *node;
  }
  inline uint64_t NumArcs() const { return arc_set_.size(); }
  inline uint64_t NumNodes() const {
    if (!FLAGS_flow_scheduling_solver.compare("flowlessly")) {
      return node_map_.size();
    } else {
      // TODO(malte): This is a work-around as cs2 and Relax IV do not allow
      // sparse node IDs, and will get tripped up
      // if current_id > graph.NumNodes().
      return current_id_;
    }
  }

 private:
  FRIEND_TEST(FlowGraphManagerTest, RemoveEquivClassNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveResourceNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveUnscheduledAggNode);
  FRIEND_TEST(FlowGraphManagerTest, TraverseAndRemoveTopology);

  uint64_t NextId();
  void PopulateUnusedIds(uint64_t new_current_id);

  unordered_set<FlowGraphArc*> arc_set_;
  // Graph structure containers and helper fields
  uint64_t current_id_;
  unordered_map<uint64_t, FlowGraphNode*> node_map_;
  // Queue storing the ids of the nodes we've previously removed.
  queue<uint64_t> unused_ids_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_H
