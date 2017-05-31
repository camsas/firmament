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
                       int64_t cost,
                       FlowGraphArcType arc_type,
                       DIMACSChangeType change_type,
                       const char* comment);
  FlowGraphArc* AddArc(uint64_t src_node_id,
                       uint64_t dst_node_id,
                       uint64_t cap_lower_bound,
                       uint64_t cap_upper_bound,
                       int64_t cost,
                       FlowGraphArcType arc_type,
                       DIMACSChangeType change_type,
                       const char* comment);
  FlowGraphNode* AddNode(FlowNodeType node_type,
                         int64_t excess,
                         DIMACSChangeType change_type,
                         const char* comment);
  void ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                 uint64_t cap_upper_bound, int64_t cost,
                 DIMACSChangeType change_type, const char* comment);
  void ChangeArcCapacity(FlowGraphArc* arc, uint64_t capacity,
                         DIMACSChangeType change_type, const char* comment);
  void ChangeArcCost(FlowGraphArc* arc, int64_t cost,
                     DIMACSChangeType change_type, const char* comment);
  void DeleteArc(FlowGraphArc* arc, DIMACSChangeType change_type,
                 const char* comment);
  void DeleteNode(FlowGraphNode* node, DIMACSChangeType change_type,
                  const char* comment);
  const vector<DIMACSChange*>& GetGraphChanges() {
    return graph_changes_;
  }
  const vector<DIMACSChange*>& GetOptimizedGraphChanges() {
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
  FRIEND_TEST(FlowGraphChangeManagerTest, AddGraphChange);
  FRIEND_TEST(FlowGraphChangeManagerTest, MergeChangesToSameArc);
  FRIEND_TEST(FlowGraphChangeManagerTest, PurgeChangesBeforeNodeRemoval);
  FRIEND_TEST(FlowGraphChangeManagerTest, RemoveDuplicateChanges);
  FRIEND_TEST(FlowGraphChangeManagerTest, ResetChanges);

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
      uint64_t cap_upper_bound, int64_t cost, FlowGraphArcType type,
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
  bool RemoveDuplicateChangesUpdateState(
      uint64_t node_id, DIMACSChange* change,
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
