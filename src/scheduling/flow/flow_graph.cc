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

#include "scheduling/flow/flow_graph.h"

DEFINE_bool(randomize_flow_graph_node_ids, false,
            "If true the the flow graph will not generate node ids in order");

namespace firmament {

FlowGraph::FlowGraph() : current_id_(1) {
  // We do not randomize the special nodes because the solvers make
  // assumptions about the the id number of the sink node.
  if (FLAGS_randomize_flow_graph_node_ids) {
    PopulateUnusedIds(50);
  }
}

FlowGraph::~FlowGraph() {
  for (unordered_map<uint64_t, FlowGraphNode*>::iterator it = node_map_.begin();
       it != node_map_.end(); ) {
    unordered_map<uint64_t, FlowGraphNode*>::iterator it_tmp = it;
    ++it;
    DeleteNode(it_tmp->second);
  }
}

FlowGraphArc* FlowGraph::AddArc(FlowGraphNode* src,
                                FlowGraphNode* dst) {
  FlowGraphArc* arc = new FlowGraphArc(src->id_, dst->id_, src, dst);
  CHECK(arc_set_.insert(arc).second);
  src->AddArc(arc);
  return arc;
}

FlowGraphArc* FlowGraph::AddArc(uint64_t src, uint64_t dst) {
  FlowGraphNode* src_node = FindPtrOrNull(node_map_, src);
  CHECK_NOTNULL(src_node);
  FlowGraphNode* dst_node = FindPtrOrNull(node_map_, dst);
  CHECK_NOTNULL(dst_node);
  FlowGraphArc* arc = new FlowGraphArc(src, dst, src_node, dst_node);
  arc_set_.insert(arc);
  src_node->AddArc(arc);
  return arc;
}

FlowGraphNode* FlowGraph::AddNode() {
  uint64_t id = NextId();
  FlowGraphNode* node = new FlowGraphNode(id);
  CHECK_NOTNULL(node);
  CHECK(InsertIfNotPresent(&node_map_, id, node));
  return node;
}

void FlowGraph::ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                          uint64_t cap_upper_bound, int64_t cost) {
  arc->cap_lower_bound_ = cap_lower_bound;
  arc->cap_upper_bound_ = cap_upper_bound;
  arc->cost_ = cost;
}

void FlowGraph::ChangeArcCost(FlowGraphArc* arc, int64_t cost) {
  ChangeArc(arc, arc->cap_lower_bound_, arc->cap_upper_bound_, cost);
}

void FlowGraph::DeleteArc(FlowGraphArc* arc) {
  // Remove the arc from the incoming and outgoing collections.
  arc->src_node_->outgoing_arc_map_.erase(arc->dst_node_->id_);
  arc->dst_node_->incoming_arc_map_.erase(arc->src_node_->id_);
  // First remove various meta-data relating to this arc
  arc_set_.erase(arc);
  // Then delete the arc itself
  delete arc;
}

void FlowGraph::DeleteNode(FlowGraphNode* node) {
  unused_ids_.push(node->id_);
  // First remove all outgoing arcs
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         node->outgoing_arc_map_.begin();
       it != node->outgoing_arc_map_.end();) {
    CHECK_EQ(it->first, it->second->dst_);
    CHECK_EQ(node->id_, it->second->src_);
    CHECK_EQ(it->second->dst_node_->incoming_arc_map_.erase(it->second->src_),
             1);
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    DeleteArc(it_tmp->second);
  }
  node->outgoing_arc_map_.clear();
  // Remove all incoming arcs.
  for (unordered_map<uint64_t, FlowGraphArc*>::iterator it =
         node->incoming_arc_map_.begin();
       it != node->incoming_arc_map_.end();) {
    CHECK_EQ(node->id_, it->second->dst_);
    CHECK_EQ(it->first, it->second->src_);
    CHECK_EQ(it->second->src_node_->outgoing_arc_map_.erase(it->second->dst_),
             1);
    unordered_map<uint64_t, FlowGraphArc*>::iterator it_tmp = it;
    ++it;
    DeleteArc(it_tmp->second);
  }
  node->incoming_arc_map_.clear();
  node_map_.erase(node->id_);
  delete node;
}

FlowGraphArc* FlowGraph::GetArc(FlowGraphNode* src, FlowGraphNode* dst) {
  CHECK_NOTNULL(src);
  CHECK_NOTNULL(dst);
  unordered_map<uint64_t, FlowGraphArc*>::iterator arc_it =
    src->outgoing_arc_map_.find(dst->id_);
  if (arc_it == src->outgoing_arc_map_.end()) {
    return NULL;
  }
  return arc_it->second;
}

uint64_t FlowGraph::NextId() {
  if (FLAGS_randomize_flow_graph_node_ids) {
    if (unused_ids_.empty()) {
      PopulateUnusedIds(current_id_ * 2);
    }
    uint64_t new_id = unused_ids_.front();
    unused_ids_.pop();
    return new_id;
  } else {
    if (unused_ids_.empty()) {
      return current_id_++;
    } else {
      uint64_t new_id = unused_ids_.front();
      unused_ids_.pop();
      return new_id;
    }
  }
}

void FlowGraph::PopulateUnusedIds(uint64_t new_current_id) {
  srand(42);
  vector<uint64_t> ids;
  for (uint64_t index = current_id_; index < new_current_id; ++index) {
    ids.push_back(index);
  }
  random_shuffle(ids.begin(), ids.end());
  for (vector<uint64_t>::iterator it = ids.begin(); it != ids.end(); ++it) {
    unused_ids_.push(*it);
  }
  current_id_ = new_current_id;
}

}  // namespace firmament
