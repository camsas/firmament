// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "scheduling/flow/flow_graph_change_manager.h"

#include "scheduling/flow/dimacs_add_node.h"
#include "scheduling/flow/dimacs_change_arc.h"
#include "scheduling/flow/dimacs_new_arc.h"
#include "scheduling/flow/dimacs_remove_node.h"

namespace firmament {

FlowGraphChangeManager::FlowGraphChangeManager(
    DIMACSChangeStats* dimacs_stats)
  : flow_graph_(new FlowGraph), dimacs_stats_(dimacs_stats) {
}

FlowGraphChangeManager::~FlowGraphChangeManager() {
  // We don't delete dimacs_stats_ because it is owned by the FlowScheduler.
  delete flow_graph_;
  ResetChanges();
}

FlowGraphArc* FlowGraphChangeManager::AddArc(FlowGraphNode* src,
                                             FlowGraphNode* dst,
                                             uint64_t cap_lower_bound,
                                             uint64_t cap_upper_bound,
                                             uint64_t cost,
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
                                             uint64_t cost,
                                             FlowGraphArcType arc_type,
                                             DIMACSChangeType change_type,
                                             const char* comment) {
  FlowGraphArc* arc = flow_graph_->AddArc(src_node_id, dst_node_id);
  arc->cap_lower_bound_ = cap_lower_bound;
  arc->cap_upper_bound_ = cap_upper_bound;
  arc->cost_ = cost;
  arc->type_ = arc_type;
  DIMACSChange* chg = new DIMACSNewArc(*arc);
  chg->set_comment(comment);
  dimacs_stats_->UpdateStats(change_type);
  AddGraphChange(chg);
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
    DIMACSChangeType change_type,
    const char* comment) {
  FlowGraphNode* node = flow_graph_->AddNode();
  node->type_ = node_type;
  DIMACSChange* chg = new DIMACSAddNode(*node, vector<FlowGraphArc*>());
  chg->set_comment(comment);
  dimacs_stats_->UpdateStats(change_type);
  AddGraphChange(chg);
  return node;
}

void FlowGraphChangeManager::ChangeArc(FlowGraphArc* arc,
                                       uint64_t cap_lower_bound,
                                       uint64_t cap_upper_bound,
                                       uint64_t cost,
                                       DIMACSChangeType change_type,
                                       const char* comment) {
  uint64_t old_cost = arc->cost_;
  if (old_cost != cost ||
      arc->cap_lower_bound_ != cap_lower_bound ||
      arc->cap_upper_bound_ != cap_upper_bound) {
    flow_graph_->ChangeArc(arc, cap_lower_bound, cap_upper_bound, cost);
    DIMACSChange* chg = new DIMACSChangeArc(*arc, old_cost);
    chg->set_comment(comment);
    dimacs_stats_->UpdateStats(change_type);
    AddGraphChange(chg);
  }
}

void FlowGraphChangeManager::ChangeArcCost(FlowGraphArc* arc,
                                           uint64_t cost,
                                           DIMACSChangeType change_type,
                                           const char* comment) {
  uint64_t old_cost = arc->cost_;
  if (old_cost != cost) {
    flow_graph_->ChangeArcCost(arc, cost);
    DIMACSChange* chg = new DIMACSChangeArc(*arc, old_cost);
    chg->set_comment(comment);
    dimacs_stats_->UpdateStats(change_type);
    AddGraphChange(chg);
  }
}

void FlowGraphChangeManager::DeleteArc(FlowGraphArc* arc,
                                       DIMACSChangeType change_type,
                                       const char* comment) {
  arc->cap_lower_bound_ = 0;
  arc->cap_upper_bound_ = 0;
  DIMACSChange *chg = new DIMACSChangeArc(*arc, arc->cost_);
  chg->set_comment(comment);
  dimacs_stats_->UpdateStats(change_type);
  AddGraphChange(chg);
  flow_graph_->DeleteArc(arc);
}

void FlowGraphChangeManager::DeleteNode(FlowGraphNode* node,
                                        DIMACSChangeType change_type,
                                        const char* comment) {
  DIMACSChange *chg = new DIMACSRemoveNode(*node);
  chg->set_comment(comment);
  dimacs_stats_->UpdateStats(change_type);
  AddGraphChange(chg);
  flow_graph_->DeleteNode(node);
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
