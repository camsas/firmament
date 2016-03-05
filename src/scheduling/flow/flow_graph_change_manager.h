// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
  FlowGraphNode* AddNode(const vector<FlowGraphArc*>& arcs,
                         FlowNodeType node_type,
                         DIMACSChangeType change_type,
                         const char* comment);
  void ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                 uint64_t cap_upper_bound, uint64_t cost,
                 DIMACSChangeType change_type, const char* comment);
  void ChangeArcCost(FlowGraphArc* arc, uint64_t cost,
                     DIMACSChangeType change_type, const char* comment);
  void DeleteArc(FlowGraphArc* arc, DIMACSChangeType change_type,
                 const char* comment);
  void DeleteNode(FlowGraphNode* node, DIMACSChangeType change_type,
                  const char* comment);
  void ResetChanges();

 private:
  void AddGraphChange(DIMACSChange* change);

  FlowGraph* flow_graph_;
  // Vector storing the graph changes occured since the last scheduling round.
  vector<DIMACSChange*> graph_changes_;
  DIMACSChangeStats* dimacs_stats_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_CHANGE_MANAGER_H
