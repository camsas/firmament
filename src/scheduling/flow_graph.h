// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Representation of a Quincy-style scheduling flow graph.

#ifndef FIRMAMENT_SCHEDULING_FLOW_GRAPH_H
#define FIRMAMENT_SCHEDULING_FLOW_GRAPH_H

#include <queue>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "engine/topology_manager.h"
#include "misc/generate_trace.h"
#include "misc/map-util.h"
#include "scheduling/dimacs_change.h"
#include "scheduling/flow_graph_arc.h"
#include "scheduling/flow_graph_node.h"
#include "scheduling/flow_scheduling_cost_model_interface.h"

DECLARE_bool(preemption);

namespace firmament {

class FlowGraph {
 public:
  explicit FlowGraph(FlowSchedulingCostModelInterface* cost_model,
                     unordered_set<ResourceID_t,
                       boost::hash<boost::uuids::uuid>>* leaf_res_ids);
  virtual ~FlowGraph();
  // Public API
  void AddMachine(ResourceTopologyNodeDescriptor* root);
  void AddOrUpdateJobNodes(JobDescriptor* jd);
  void AddResourceTopology(
      ResourceTopologyNodeDescriptor* resource_tree);
  void AdjustUnscheduledAggArcCosts();
  void ChangeArc(FlowGraphArc* arc, uint64_t cap_lower_bound,
                 uint64_t cap_upper_bound, uint64_t cost);
  bool CheckNodeType(uint64_t node, FlowNodeType_NodeType type);
  void JobCompleted(JobID_t job_id);
  FlowGraphNode* NodeForResourceID(const ResourceID_t& res_id);
  FlowGraphNode* NodeForTaskID(TaskID_t task_id);
  void RemoveMachine(ResourceID_t res_id);
  void ResetChanges();
  // TODO(ionel): Call TaskCompleted from scheduler.
  void TaskCompleted(TaskID_t task_id);
  void TaskEvicted(TaskID_t task_id, ResourceID_t res_id);
  void TaskFailed(TaskID_t task_id);
  void TaskKilled(TaskID_t task_id);
  void TaskScheduled(TaskID_t task_id, ResourceID_t res_id);
  void ComputeTopologyStatistics(
    FlowGraphNode* node,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> gather);
  void UpdateResourceTopology(
      ResourceTopologyNodeDescriptor* resource_tree);
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
  inline const unordered_set<uint64_t>& unsched_agg_ids() const {
    return unsched_agg_nodes_;
  }
  inline const FlowGraphNode& cluster_agg_node() const {
    return *cluster_agg_node_;
  }
  inline uint64_t NumArcs() const { return arc_set_.size(); }
  inline uint64_t NumNodes() const { return current_id_ - 1; }
  inline FlowGraphNode* Node(uint64_t id) {
    FlowGraphNode* const* npp = FindOrNull(node_map_, id);
    return (npp ? *npp : NULL);
  }
  inline vector<DIMACSChange*>& graph_changes() { return graph_changes_; }
  inline FlowGraphNode* sink_node() {
    return sink_node_;
  }

 protected:
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(DIMACSExporterTest, ScalabilityTestGraphs);
  FRIEND_TEST(FlowGraphTest, AddArcToNode);
  FRIEND_TEST(FlowGraphTest, AddOrUpdateJobNodes);
  FRIEND_TEST(FlowGraphTest, AddResourceNode);
  FRIEND_TEST(FlowGraphTest, ChangeArc);
  FRIEND_TEST(FlowGraphTest, DeleteTaskNode);
  FRIEND_TEST(FlowGraphTest, DeleteResourceNode);
  FRIEND_TEST(FlowGraphTest, ResetChanges);
  FRIEND_TEST(FlowGraphTest, UnschedAggCapacityAdjustment);
  void AddArcsForTask(FlowGraphNode* task_node, FlowGraphNode* unsched_agg_node,
                      vector<FlowGraphArc*>* task_arcs);
  FlowGraphArc* AddArcInternal(FlowGraphNode* src, FlowGraphNode* dst);
  void AddArcsFromToOtherEquivNodes(EquivClass_t equiv_class,
                                    FlowGraphNode* ec_node);
  FlowGraphNode* AddNodeInternal(uint64_t id);
  FlowGraphArc* AddArcInternal(uint64_t src, uint64_t dst);
  void AddEquivClassNode(EquivClass_t ec);
  void AddResourceEquivClasses(FlowGraphNode* res_node);
  void AddResourceNode(ResourceTopologyNodeDescriptor* rtnd);
  void AddSpecialNodes();
  void AddTaskEquivClasses(FlowGraphNode* task_node);
  void AdjustUnscheduledAggToSinkCapacityGeneratingDelta(
      JobID_t job, int64_t delta);
  void ConfigureResourceRootNode(const ResourceTopologyNodeDescriptor& rtnd,
                                 FlowGraphNode* new_node);
  void ConfigureResourceBranchNode(const ResourceTopologyNodeDescriptor& rtnd,
                                   FlowGraphNode* new_node);
  void ConfigureResourceLeafNode(const ResourceTopologyNodeDescriptor& rtnd,
                                 FlowGraphNode* new_node);
  void DeleteArcGeneratingDelta(FlowGraphArc* arc);
  void DeleteArc(FlowGraphArc* arc);
  void DeleteNode(FlowGraphNode* node);
  void DeleteResourceNode(FlowGraphNode* res_node);
  void DeleteTaskNode(TaskID_t task_id);
  void DeleteOrUpdateIncomingEquivNode(EquivClass_t task_equiv);
  void DeleteOrUpdateOutgoingEquivNode(EquivClass_t task_equiv);
  FlowGraphNode* GetUnschedAggForJob(JobID_t job_id);
  uint64_t NextId();
  void PinTaskToNode(FlowGraphNode* task_node, FlowGraphNode* res_node);
  void PopulateUnusedIds(uint64_t new_current_id);
  void RemoveMachineSubTree(FlowGraphNode* res_node);
  void UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id);
  void UpdateArcsForEvictedTask(TaskID_t task_id, ResourceID_t res_id);
  void UpdateResourceNode(ResourceTopologyNodeDescriptor* rtnd);

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
  // The "node ID" for the job is currently the ID of the job's unscheduled node
  unordered_map<JobID_t, uint64_t,
      boost::hash<boost::uuids::uuid> > job_unsched_to_node_id_;
  unordered_set<uint64_t> leaf_nodes_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  unordered_set<uint64_t> task_nodes_;
  unordered_set<uint64_t> unsched_agg_nodes_;

  // Mapping storing flow graph nodes for each task equivalence class.
  unordered_map<EquivClass_t, FlowGraphNode*> tec_to_node_;

  // Vector storing the graph changes occured since the last scheduling round.
  vector<DIMACSChange*> graph_changes_;
  // Queue storing the ids of the nodes we've previously removed.
  queue<uint64_t> unused_ids_;
  // Vector storing the ids of the nodes we've created.
  vector<uint64_t> ids_created_;
  GenerateTrace generate_trace_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_GRAPH_H
