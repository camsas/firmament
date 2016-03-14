// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// The FlowGraphChangeManager bridges FlowGraphManager and FlowGraph. Every
// graph change done by the FlowGraphManager should be conducted via
// FlowGraphChangeManager's methods.
// The class stores all the changes conducted in-between two scheduling rounds.
// Moreover, FlowGraphChangeManager applies various algorithms to reduce
// the number of changes (e.g., merges idempotent changes, removes superfluous
// changes).

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_CHANGE_MANAGER_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_CHANGE_MANAGER_H

#include "base/types.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"

namespace firmament {

class FlowGraphChangeManager {
 public:
  FlowGraphChangeManager(DIMACSChangeStats* dimacs_stats);
  ~FlowGraphChangeManager();
  FlowGraphArc* AddArc(FlowGraphNode* src,
                       FlowGraphNode* dst,
                       uint64_t cap_lower_bound,
                       uint64_t cap_upper_bound,
                       uint64_t cost,
                       FlowGraphArcType arc_type,
                       DIMACSChangeType change_type,
                       const char* comment);
  FlowGraphArc* AddArc(uint64_t src_node_id,
                       uint64_t dst_node_id,
                       uint64_t cap_lower_bound,
                       uint64_t cap_upper_bound,
                       uint64_t cost,
                       FlowGraphArcType arc_type,
                       DIMACSChangeType change_type,
                       const char* comment);
  FlowGraphNode* AddNode(FlowNodeType node_type,
                         int64_t excess,
                         DIMACSChangeType change_type,
                         const char* comment);
  void ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                 uint64_t cap_upper_bound, uint64_t cost,
                 DIMACSChangeType change_type, const char* comment);
  void ChangeArcCapacity(FlowGraphArc* arc, uint64_t capacity,
                         DIMACSChangeType change_type, const char* comment);
  void ChangeArcCost(FlowGraphArc* arc, uint64_t cost,
                     DIMACSChangeType change_type, const char* comment);
  void DeleteArc(FlowGraphArc* arc, DIMACSChangeType change_type,
                 const char* comment);
  void DeleteNode(FlowGraphNode* node, DIMACSChangeType change_type,
                  const char* comment);
  const vector<DIMACSChange*>& GetGraphChanges() {
    OptimizeChanges();
    return graph_changes_;
  }
  void ResetChanges();
  inline bool CheckNodeType(uint64_t node_id, FlowNodeType type) {
    return flow_graph_->Node(node_id).type_ == type;
  }
  inline const FlowGraph& flow_graph() {
    return *flow_graph_;
  }
  inline FlowGraph* mutable_flow_graph() {
    return flow_graph_;
  }
  inline const FlowGraphNode& Node(uint64_t node_id) {
    return flow_graph_->Node(node_id);
  }

 private:
  FRIEND_TEST(FlowGraphChangeManagerTest, MergeChangesToSameArc);
  FRIEND_TEST(FlowGraphChangeManagerTest, PurgeChangesBeforeNodeRemoval);
  FRIEND_TEST(FlowGraphChangeManagerTest, RemoveDuplicateChanges);

  void AddGraphChange(DIMACSChange* change);
  void OptimizeChanges();
  void MergeChangesToSameArc();
  /**
   * Checks if there's already a change for the (src_id, dst_id) arc.
   * If there's no change then it adds one to the state, otherwise
   * it updates the existing change.
   */
  void MergeChangesToSameArcHelper(
      uint64_t src_id, uint64_t dst_id, uint64_t cap_lower_bound,
      uint64_t cap_upper_bound, uint64_t cost, FlowGraphArcType type,
      DIMACSChange* change, vector<DIMACSChange*>* new_graph_changes,
      unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>*
      arcs_src_changes,
      unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>*
      arcs_dst_changes);
  void PurgeChangesBeforeNodeRemoval();
  void RemoveDuplicateChanges();
  /**
   * Checks if there's already an identical change for the (src_id, dst_id) arc.
   * If there's no change the it updates the state, otherwise it just ignores
   * the change we're currently processing because it's duplicate.
   */
  void RemoveDuplicateChangesHelper(
      uint64_t src_id, uint64_t dst_id, DIMACSChange* change,
      vector<DIMACSChange*>* new_graph_changes,
      unordered_map<uint64_t, unordered_map<string, DIMACSChange*>>*
      node_to_change);
  /**
   * Method to be called upon node addition. This method makes sure that the
   * state is cleaned when we re-use a node id.
   */
  void RemoveDuplicateCleanState(
      uint64_t new_node_id, uint64_t src, uint64_t dst,
      const string& change_desc,
      unordered_map<uint64_t, unordered_map<string, DIMACSChange*>>*
      node_to_change);

  FlowGraph* flow_graph_;
  // Vector storing the graph changes occured since the last scheduling round.
  vector<DIMACSChange*> graph_changes_;
  DIMACSChangeStats* dimacs_stats_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_CHANGE_MANAGER_H
