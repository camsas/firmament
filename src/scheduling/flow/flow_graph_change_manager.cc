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

#include "scheduling/flow/flow_graph_change_manager.h"

#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"

DEFINE_bool(remove_duplicate_changes, true,
            "True if duplicate DIMACS changes should be removed");
DEFINE_bool(merge_changes_to_same_arc, true, "True if changes to the same arc "
            "in the flow graph should be merged");
DEFINE_bool(purge_changes_before_node_removal, true,
            "True if changes on incoming/outgoing arcs of a node that is going "
            "to be removed should be purged");

DECLARE_bool(incremental_flow);

namespace firmament {

FlowGraphChangeManager::FlowGraphChangeManager(
    DIMACSChangeStats* dimacs_stats)
  : flow_graph_(new FlowGraph), dimacs_stats_(dimacs_stats) {
}

FlowGraphChangeManager::~FlowGraphChangeManager() {
  // We don't delete dimacs_stats_ because it is owned by the FlowScheduler.
  delete flow_graph_;
  ResetChanges();
  // XXX(malte): N.B. this leaks memory as we haven't destroyed all of the
  // nodes and arcs in the flow graph (which are allocated on the heap)
}

FlowGraphArc* FlowGraphChangeManager::AddArc(FlowGraphNode* src,
                                             FlowGraphNode* dst,
                                             uint64_t cap_lower_bound,
                                             uint64_t cap_upper_bound,
                                             int64_t cost,
                                             FlowGraphArcType arc_type,
                                             DIMACSChangeType change_type,
                                             const char* comment) {
  return AddArc(src->id_, dst->id_, cap_lower_bound, cap_upper_bound, cost,
                arc_type, change_type, comment);
}

FlowGraphArc* FlowGraphChangeManager::AddArc(uint64_t src_node_id,
                                             uint64_t dst_node_id,
                                             uint64_t cap_lower_bound,
                                             uint64_t cap_upper_bound,
                                             int64_t cost,
                                             FlowGraphArcType arc_type,
                                             DIMACSChangeType change_type,
                                             const char* comment) {
  FlowGraphArc* arc = flow_graph_->AddArc(src_node_id, dst_node_id);
  arc->cap_lower_bound_ = cap_lower_bound;
  arc->cap_upper_bound_ = cap_upper_bound;
  arc->cost_ = cost;
  arc->type_ = arc_type;
  if (FLAGS_incremental_flow) {
    DIMACSChange* chg = new DIMACSNewArc(*arc);
    chg->set_comment(comment);
    AddGraphChange(chg);
  }
  dimacs_stats_->UpdateStats(change_type);
  return arc;
}

void FlowGraphChangeManager::AddGraphChange(DIMACSChange* change) {
  if (change->comment().empty()) {
    change->set_comment("AddGraphChange: anonymous caller");
  }
  graph_changes_.push_back(change);
}

FlowGraphNode* FlowGraphChangeManager::AddNode(
    FlowNodeType node_type,
    int64_t excess,
    DIMACSChangeType change_type,
    const char* comment) {
  FlowGraphNode* node = flow_graph_->AddNode();
  node->type_ = node_type;
  node->excess_ = excess;
  node->comment_ = comment;
  if (FLAGS_incremental_flow) {
    DIMACSChange* chg = new DIMACSAddNode(*node, vector<FlowGraphArc*>());
    chg->set_comment(comment);
    AddGraphChange(chg);
  }
  dimacs_stats_->UpdateStats(change_type);
  return node;
}

void FlowGraphChangeManager::ChangeArc(FlowGraphArc* arc,
                                       uint64_t cap_lower_bound,
                                       uint64_t cap_upper_bound,
                                       int64_t cost,
                                       DIMACSChangeType change_type,
                                       const char* comment) {
  CHECK_NOTNULL(arc);
  CHECK_GE(arc->cap_upper_bound_, 0);
  int64_t old_cost = arc->cost_;
  if (old_cost != cost ||
      arc->cap_lower_bound_ != cap_lower_bound ||
      arc->cap_upper_bound_ != cap_upper_bound) {
    flow_graph_->ChangeArc(arc, cap_lower_bound, cap_upper_bound, cost);
    if (FLAGS_incremental_flow) {
      DIMACSChange* chg = new DIMACSChangeArc(*arc, old_cost);
      chg->set_comment(comment);
      AddGraphChange(chg);
    }
    dimacs_stats_->UpdateStats(change_type);
  }
}

void FlowGraphChangeManager::ChangeArcCapacity(FlowGraphArc* arc,
                                               uint64_t capacity,
                                               DIMACSChangeType change_type,
                                               const char* comment) {
  CHECK_NOTNULL(arc);
  uint64_t old_capacity = arc->cap_upper_bound_;
  if (old_capacity != capacity) {
    flow_graph_->ChangeArc(arc, arc->cap_lower_bound_, capacity, arc->cost_);
    if (FLAGS_incremental_flow) {
      DIMACSChange* chg = new DIMACSChangeArc(*arc, arc->cost_);
      chg->set_comment(comment);
      AddGraphChange(chg);
    }
    dimacs_stats_->UpdateStats(change_type);
  }
}

void FlowGraphChangeManager::ChangeArcCost(FlowGraphArc* arc,
                                           int64_t cost,
                                           DIMACSChangeType change_type,
                                           const char* comment) {
  CHECK_NOTNULL(arc);
  int64_t old_cost = arc->cost_;
  if (old_cost != cost) {
    flow_graph_->ChangeArcCost(arc, cost);
    if (FLAGS_incremental_flow) {
      DIMACSChange* chg = new DIMACSChangeArc(*arc, old_cost);
      chg->set_comment(comment);
      AddGraphChange(chg);
    }
    dimacs_stats_->UpdateStats(change_type);
  }
}

void FlowGraphChangeManager::DeleteArc(FlowGraphArc* arc,
                                       DIMACSChangeType change_type,
                                       const char* comment) {
  arc->cap_lower_bound_ = 0;
  arc->cap_upper_bound_ = 0;
  if (FLAGS_incremental_flow) {
    DIMACSChange *chg = new DIMACSChangeArc(*arc, arc->cost_);
    chg->set_comment(comment);
    AddGraphChange(chg);
  }
  dimacs_stats_->UpdateStats(change_type);
  flow_graph_->DeleteArc(arc);
}

void FlowGraphChangeManager::DeleteNode(FlowGraphNode* node,
                                        DIMACSChangeType change_type,
                                        const char* comment) {
  if (FLAGS_incremental_flow) {
    DIMACSChange *chg = new DIMACSRemoveNode(*node);
    chg->set_comment(comment);
    AddGraphChange(chg);
  }
  dimacs_stats_->UpdateStats(change_type);
  flow_graph_->DeleteNode(node);
}

void FlowGraphChangeManager::MergeChangesToSameArcHelper(
    uint64_t src_id, uint64_t dst_id, uint64_t cap_lower_bound,
    uint64_t cap_upper_bound, int64_t cost, FlowGraphArcType type,
    DIMACSChange* change, vector<DIMACSChange*>* new_graph_changes,
    unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>*
    arcs_src_changes,
    unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>*
    arcs_dst_changes) {
  CHECK_NOTNULL(change);
  unordered_map<uint64_t, DIMACSChange*>* dst_to_change =
    FindOrNull(*arcs_src_changes, src_id);
  if (dst_to_change) {
    // We already have at least one arc starting at src.
    DIMACSChange* chg = FindPtrOrNull(*dst_to_change, dst_id);
    if (chg) {
      // Update the existing entry.
      if (DIMACSChangeArc* chg_arc = dynamic_cast<DIMACSChangeArc*>(chg)) {
        chg_arc->cap_lower_bound_ = cap_lower_bound;
        chg_arc->cap_upper_bound_ = cap_upper_bound;
        chg_arc->cost_ = cost;
        chg_arc->type_ = type;
        // We don't update the old_cost on a merge because we want to keep the
        // first recorded old cost value which is the value that the solver
        // currently has for the arc.
      } else if (DIMACSNewArc* new_arc = dynamic_cast<DIMACSNewArc*>(chg)) {
        new_arc->cap_lower_bound_ = cap_lower_bound;
        new_arc->cap_upper_bound_ = cap_upper_bound;
        new_arc->cost_ = cost;
        new_arc->type_ = type;
      } else {
        LOG(FATAL) << "Unexpected type of change";
      }
    } else {
      // We don't have an entry for the arc.
      CHECK(InsertIfNotPresent(dst_to_change, dst_id, change));
      new_graph_changes->push_back(change);
    }
  } else {
    // No arc starting at source has been changed.
    unordered_map<uint64_t, DIMACSChange*> new_dst_to_change;
    InsertIfNotPresent(&new_dst_to_change, dst_id, change);
    InsertIfNotPresent(arcs_src_changes, src_id, new_dst_to_change);
    // Append the change to the change list because it's the first time we
    // update this arc.
    new_graph_changes->push_back(change);
  }
  unordered_map<uint64_t, DIMACSChange*>* src_to_change =
    FindOrNull(*arcs_dst_changes, dst_id);
  if (src_to_change) {
    // Only insert the change if we don't already have one for the arc.
    InsertIfNotPresent(src_to_change, src_id, change);
  } else {
    unordered_map<uint64_t, DIMACSChange*> new_src_to_change;
    InsertIfNotPresent(&new_src_to_change, src_id, change);
    InsertIfNotPresent(arcs_dst_changes, dst_id, new_src_to_change);
  }
}

void FlowGraphChangeManager::MergeChangesToSameArc() {
  vector<DIMACSChange*> new_graph_changes;
  // For each arc change we have an entry in arcs_src_changes and one in
  // arcs_dst_changes. arcs_src_changes is indexed by src_id and then by
  // dst_id. arcs_dst_changes is indexed by dst_id and then by src_id.
  // These two maps allow us to get all the arcs that have a particular
  // node as a source or destination.
  unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>
    arcs_src_changes;
  unordered_map<uint64_t, unordered_map<uint64_t, DIMACSChange*>>
    arcs_dst_changes;
  for (auto& change : graph_changes_) {
    if (DIMACSChangeArc* chg_arc = dynamic_cast<DIMACSChangeArc*>(change)) {
      // Check if we can merge the arc change.
      MergeChangesToSameArcHelper(
          chg_arc->src_, chg_arc->dst_, chg_arc->cap_lower_bound_,
          chg_arc->cap_upper_bound_, chg_arc->cost_, chg_arc->type_, change,
          &new_graph_changes, &arcs_src_changes, &arcs_dst_changes);
    } else if (DIMACSNewArc* new_arc = dynamic_cast<DIMACSNewArc*>(change)) {
      // Check if we can merge the arc change.
      MergeChangesToSameArcHelper(
          new_arc->src_, new_arc->dst_, new_arc->cap_lower_bound_,
          new_arc->cap_upper_bound_, new_arc->cost_, new_arc->type_, change,
          &new_graph_changes, &arcs_src_changes, &arcs_dst_changes);
    } else if (DIMACSAddNode* new_node = dynamic_cast<DIMACSAddNode*>(change)) {
      // Remove all the arcs that have new_node->id_ as source or destination.
      unordered_map<uint64_t, DIMACSChange*>* dst_to_change =
        FindOrNull(arcs_src_changes, new_node->id_);
      if (dst_to_change) {
        for (auto& dst_change : *dst_to_change) {
          unordered_map<uint64_t, DIMACSChange*>* src_to_change =
            FindOrNull(arcs_dst_changes, dst_change.first);
          CHECK_NOTNULL(src_to_change);
          src_to_change->erase(new_node->id_);
        }
      }
      arcs_src_changes.erase(new_node->id_);
      arcs_dst_changes.erase(new_node->id_);
      new_graph_changes.push_back(change);
    } else if (dynamic_cast<DIMACSRemoveNode*>(change)) {
      new_graph_changes.push_back(change);
    } else {
      LOG(FATAL) << "Unexpected type of change";
    }
  }
  graph_changes_.clear();
  graph_changes_.insert(graph_changes_.begin(), new_graph_changes.begin(),
                        new_graph_changes.end());
}

void FlowGraphChangeManager::OptimizeChanges() {
  if (FLAGS_remove_duplicate_changes) {
    RemoveDuplicateChanges();
  }
  if (FLAGS_merge_changes_to_same_arc) {
    MergeChangesToSameArc();
  }
  if (FLAGS_purge_changes_before_node_removal) {
    PurgeChangesBeforeNodeRemoval();
  }
}

void FlowGraphChangeManager::PurgeChangesBeforeNodeRemoval() {
  // Set in which we store the ids of the nodes that are removed. We process
  // the changes from the last to the first one. Whenever we encounter a
  // remove node change we put its node id to the set. Similarly, whenever
  // we encounter an add node change we remove its node id from the set.
  // In this way we make sure we handle the case when ids are re-used upon
  // node addition.
  unordered_set<uint64_t> nodes_removed;
  list<DIMACSChange*> new_graph_changes;
  for (vector<DIMACSChange*>::reverse_iterator rev_it = graph_changes_.rbegin();
       rev_it != graph_changes_.rend(); ++rev_it) {
    if (DIMACSRemoveNode* rem_node =
        dynamic_cast<DIMACSRemoveNode*>(*rev_it)) {
      if (nodes_removed.insert(rem_node->node_id_).second) {
        // Only add the change if the node has not been previosly inserted into
        // nodes_removed. It if has been inserted then there's no point to add
        // the change because the node is going to be removed in a future
        // change.
        new_graph_changes.push_front(rem_node);
      }
    } else if (DIMACSAddNode* new_node =
               dynamic_cast<DIMACSAddNode*>(*rev_it)) {
      nodes_removed.erase(new_node->id_);
      new_graph_changes.push_front(new_node);
    } else if (DIMACSNewArc* new_arc =
               dynamic_cast<DIMACSNewArc*>(*rev_it)) {
      if (nodes_removed.find(new_arc->src_) == nodes_removed.end() &&
          nodes_removed.find(new_arc->dst_) == nodes_removed.end()) {
        // Only add the change if neither of its arc source or destination
        // nodes are going to be removed.
        new_graph_changes.push_front(new_arc);
      }
    } else if (DIMACSChangeArc* chg_arc =
               dynamic_cast<DIMACSChangeArc*>(*rev_it)) {
      if (nodes_removed.find(chg_arc->src_) == nodes_removed.end() &&
          nodes_removed.find(chg_arc->dst_) == nodes_removed.end()) {
        // Only add the change if neither of its arc source or destination
        // nodes are going to be removed.
        new_graph_changes.push_front(chg_arc);
      }
    } else {
      LOG(FATAL) << "Unexpected type of change";
    }
  }
  graph_changes_.clear();
  graph_changes_.insert(graph_changes_.begin(), new_graph_changes.begin(),
                        new_graph_changes.end());
}

void FlowGraphChangeManager::RemoveDuplicateChanges() {
  vector<DIMACSChange*> new_graph_changes;
  // For each arc change we have two entries in node_to_change. One entry
  // which is indexed by src_id and another one with is indexed by dst_id.
  // Thus, whenever we re-use a node id we can make sure to remove from the
  // state all the arcs that connect the previously removed node that was
  // using the same id.
  unordered_map<uint64_t, unordered_map<string, DIMACSChange*>> node_to_change;
  for (auto& change : graph_changes_) {
    if (DIMACSChangeArc* chg_arc = dynamic_cast<DIMACSChangeArc*>(change)) {
      RemoveDuplicateChangesHelper(chg_arc->src_, chg_arc->dst_, change,
                                   &new_graph_changes, &node_to_change);
    } else if (DIMACSNewArc* new_arc = dynamic_cast<DIMACSNewArc*>(change)) {
      RemoveDuplicateChangesHelper(new_arc->src_, new_arc->dst_, change,
                                   &new_graph_changes, &node_to_change);
    } else if (DIMACSAddNode* new_node = dynamic_cast<DIMACSAddNode*>(change)) {
      // Remove from the state the arc changes that connect a node with the
      // same id as the node we're adding.
      unordered_map<string, DIMACSChange*>* desc_to_change =
        FindOrNull(node_to_change, new_node->id_);
      if (desc_to_change) {
        for (auto& desc_change : *desc_to_change) {
          if (DIMACSNewArc* new_arc =
              dynamic_cast<DIMACSNewArc*>(desc_change.second)) {
            RemoveDuplicateCleanState(new_node->id_, new_arc->src_,
                                      new_arc->dst_, desc_change.first,
                                      &node_to_change);
          } else if (DIMACSChangeArc* chg_arc =
                     dynamic_cast<DIMACSChangeArc*>(desc_change.second)) {
            RemoveDuplicateCleanState(new_node->id_, chg_arc->src_,
                                      chg_arc->dst_, desc_change.first,
                                      &node_to_change);
          } else {
            LOG(FATAL) << "Unexpected change type";
          }
        }
      }
      node_to_change.erase(new_node->id_);
      new_graph_changes.push_back(change);
    } else if (dynamic_cast<DIMACSRemoveNode*>(change)) {
      new_graph_changes.push_back(change);
    }
  }
  graph_changes_.clear();
  graph_changes_.insert(graph_changes_.begin(), new_graph_changes.begin(),
                        new_graph_changes.end());
}

void FlowGraphChangeManager::RemoveDuplicateChangesHelper(
    uint64_t src_id, uint64_t dst_id, DIMACSChange* change,
    vector<DIMACSChange*>* new_graph_changes,
    unordered_map<uint64_t, unordered_map<string, DIMACSChange*>>*
    node_to_change) {
  if (RemoveDuplicateChangesUpdateState(src_id, change, node_to_change) &&
      RemoveDuplicateChangesUpdateState(dst_id, change, node_to_change)) {
    new_graph_changes->push_back(change);
  }
}

bool FlowGraphChangeManager::RemoveDuplicateChangesUpdateState(
    uint64_t node_id, DIMACSChange* change,
    unordered_map<uint64_t, unordered_map<string, DIMACSChange*>>*
    node_to_change) {
  string change_desc = change->GenerateChange();
  unordered_map<string, DIMACSChange*>* changes =
    FindOrNull(*node_to_change, node_id);
  if (changes) {
    if (!InsertIfNotPresent(changes, change_desc, change)) {
      // The change is already in the map.
      return false;
    }
  } else {
    unordered_map<string, DIMACSChange*> new_changes;
    InsertIfNotPresent(&new_changes, change_desc, change);
    InsertIfNotPresent(node_to_change, node_id, new_changes);
  }
  return true;
}

void FlowGraphChangeManager::RemoveDuplicateCleanState(
    uint64_t new_node_id, uint64_t src, uint64_t dst, const string& change_desc,
    unordered_map<uint64_t, unordered_map<string, DIMACSChange*>>*
    node_to_change) {
  if (new_node_id == src) {
    auto dst_change_map = FindOrNull(*node_to_change, dst);
    CHECK_NOTNULL(dst_change_map);
    dst_change_map->erase(change_desc);
    if (dst_change_map->size() == 0) {
      node_to_change->erase(dst);
    }
  } else if (new_node_id == dst) {
    auto src_change_map = FindOrNull(*node_to_change, src);
    CHECK_NOTNULL(src_change_map);
    src_change_map->erase(change_desc);
    if (src_change_map->size() == 0) {
      node_to_change->erase(src);
    }
  } else {
    LOG(FATAL) << "New node id is not equal to the source or destination ids";
  }
}

void FlowGraphChangeManager::ResetChanges() {
  for (vector<DIMACSChange*>::iterator it = graph_changes_.begin();
       it != graph_changes_.end(); ) {
    vector<DIMACSChange*>::iterator it_tmp = it;
    ++it;
    delete *it_tmp;
  }
  graph_changes_.clear();
}

}  // namespace firmament
