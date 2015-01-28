// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#ifndef FIRMAMENT_SCHEDULING_FLOW_GRAPH_H
#define FIRMAMENT_SCHEDULING_FLOW_GRAPH_H

#include <queue>
#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "engine/topology_manager.h"
#include "misc/equivclasses.h"
#include "misc/map-util.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

namespace firmament {

class FlowGraph {
 public:
  FlowGraph(FlowSchedulingCostModelInterface* cost_model);
  virtual ~FlowGraph();
  // Public API
  void AddJobNodes(JobDescriptor* jd);
  void AddResourceNode(const ResourceTopologyNodeDescriptor& rtnd);
  void AddResourceTopology(
      const ResourceTopologyNodeDescriptor& resource_tree);
  void AddTaskNode();
  void DeleteTaskNode(const TaskDescriptor& td);
  void DeleteTaskNode(TaskID_t task_id);
  void DeleteResourceNode(const ResourceDescriptor& rd);
  void DeleteNodesForJob(const JobDescriptor& jd);
  FlowGraphNode* GetUnschedAggForJob(JobID_t job_id);
  FlowGraphNode* NodeForResourceID(const ResourceID_t& res_id);
  FlowGraphNode* NodeForTaskID(TaskID_t task_id);
  void UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id);
  void UpdateResourceNode(const ResourceTopologyNodeDescriptor& rtnd);
  void UpdateResourceTopology(
      const ResourceTopologyNodeDescriptor& resource_tree);
  void ResetChanges();

  // Simple accessor methods
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
  //inline uint64_t NumNodes() const { return node_map_.size(); }
  inline uint64_t NumNodes() const { return current_id_ - 1; }
  inline FlowGraphNode* Node(uint64_t id) {
    FlowGraphNode* const* npp = FindOrNull(node_map_, id);
    return (npp ? *npp : NULL);
  }
  inline vector<DIMACSChange>& graph_changes() { return graph_changes_; };

 protected:
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(DIMACSExporterTest, ScalabilityTestGraphs);
  FRIEND_TEST(FlowGraphTest, AddArcToNode);
  FRIEND_TEST(FlowGraphTest, UnschedAggCapacityAdjustment);
  void AddArcsForTask(FlowGraphNode* task_node,
                      FlowGraphNode* unsched_agg_node);
  FlowGraphArc* AddArcInternal(FlowGraphNode* src, FlowGraphNode* dst);
  FlowGraphNode* AddNodeInternal(uint64_t id);
  FlowGraphArc* AddArcInternal(uint64_t src, uint64_t dst);
  FlowGraphNode* AddEquivClassAggregator(TaskEquivClass_t equivclass);
  void AddSpecialNodes();
  void AdjustUnscheduledAggToSinkCapacity(JobID_t job, int64_t delta);
  void ConfigureResourceRootNode(const ResourceTopologyNodeDescriptor& rtnd,
                                 FlowGraphNode* new_node);
  void ConfigureResourceBranchNode(const ResourceTopologyNodeDescriptor& rtnd,
                                   FlowGraphNode* new_node);
  void ConfigureResourceLeafNode(const ResourceTopologyNodeDescriptor& rtnd,
                                 FlowGraphNode* new_node);
  void DeleteArc(FlowGraphArc* arc);
  void DeleteNode(FlowGraphNode* node);
  void PinTaskToNode(FlowGraphNode* task_node, FlowGraphNode* res_node);

  uint64_t next_id() {
    return current_id_++;
  }

  // Flow scheduling cost model used
  FlowSchedulingCostModelInterface* cost_model_;

  // Graph structure containers and helper fields
  uint64_t current_id_;
  unordered_map<uint64_t, FlowGraphNode*> node_map_;
  unordered_set<FlowGraphArc*> arc_set_;
  FlowGraphNode* cluster_agg_node_;
  FlowGraphNode* sink_node_;
  // Resource and task mappings
  unordered_map<TaskID_t, uint64_t> task_to_nodeid_map_;
  unordered_map<ResourceID_t, uint64_t,
      boost::hash<boost::uuids::uuid> > resource_to_nodeid_map_;
  // Hacky solution for retrieval of the parent of any particular resource
  // (needed to assign capacities properly by back-tracking).
  unordered_map<ResourceID_t, ResourceID_t,
      boost::hash<boost::uuids::uuid> > resource_to_parent_map_;
  // Hacky equivalence class node map
  unordered_map<TaskEquivClass_t, uint64_t> equiv_class_to_nodeid_map_;
  // The "node ID" for the job is currently the ID of the job's unscheduled node
  unordered_map<JobID_t, uint64_t,
      boost::hash<boost::uuids::uuid> > job_to_nodeid_map_;
  unordered_set<uint64_t> leaf_nodes_;
  unordered_set<uint64_t> task_nodes_;
  // Vector storing the graph changes occured since the last scheduling round.
  vector<DIMACSChange> graph_changes_;
  // Vector storing the ids of the nodes we've previously removed.
  queue<uint64_t> unused_ids_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_GRAPH_H
