// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H

#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/generate_trace.h"
#include "misc/map-util.h"
#include "misc/time_interface.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_node.h"

DECLARE_bool(preemption);
DECLARE_string(flow_scheduling_solver);

namespace firmament {

class FlowGraphManager {
 public:
  explicit FlowGraphManager(CostModelInterface* cost_model,
                            unordered_set<ResourceID_t,
                              boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                            TimeInterface* time_manager,
                            GenerateTrace* generate_trace,
                            DIMACSChangeStats* dimacs_stats);
  virtual ~FlowGraphManager();
  // Public API
  void AddGraphChange(DIMACSChange* change);
  void AddMachine(ResourceTopologyNodeDescriptor* root);
  void AddOrUpdateJobNodes(JobDescriptor* jd);
  void AddResourceTopology(
      ResourceTopologyNodeDescriptor* resource_tree);
  bool CheckNodeType(uint64_t node, FlowNodeType type);
  void ComputeTopologyStatistics(
    FlowGraphNode* node,
    boost::function<void(FlowGraphNode*)> prepare,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> gather,
    boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> update);
  void JobCompleted(JobID_t job_id);
  FlowGraphNode* NodeForResourceID(const ResourceID_t& res_id);
  FlowGraphNode* NodeForTaskID(TaskID_t task_id);
  void RemoveMachine(const ResourceDescriptor& rd, set<uint64_t>* pus_removed);
  void ResetChanges();
  uint64_t TaskCompleted(TaskID_t task_id);
  void TaskEvicted(TaskID_t task_id, ResourceID_t res_id);
  void TaskFailed(TaskID_t task_id);
  void TaskKilled(TaskID_t task_id);
  void TaskMigrated(TaskID_t task_id,
                    ResourceID_t old_res_id,
                    ResourceID_t new_res_id);
  void TaskScheduled(TaskID_t task_id, ResourceID_t res_id);
  FlowGraphNode* UnscheduledAggregatorForJobID(JobID_t job_id);
  void UpdateResourceTopology(
      ResourceTopologyNodeDescriptor* resource_tree);
  void UpdateTimeDependentCosts(vector<JobDescriptor*>* job_vec);
  void UpdateUnscheduledAggArcCosts();
  // Simple accessor methods
  inline FlowGraph* flow_graph() {
    return flow_graph_;
  }
  inline vector<DIMACSChange*>& graph_changes() {
    return graph_changes_;
  }
  inline const unordered_set<uint64_t>& leaf_node_ids() const {
    return leaf_nodes_;
  }
  inline const unordered_set<uint64_t>& task_node_ids() const {
    return task_nodes_;
  }
  inline FlowGraphNode* sink_node() {
    return sink_node_;
  }

 protected:
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(DIMACSExporterTest, ScalabilityTestGraphs);
  FRIEND_TEST(FlowGraphManagerTest, AddOrUpdateJobNodes);
  FRIEND_TEST(FlowGraphManagerTest, AddOrUpdateResourceNode);
  FRIEND_TEST(FlowGraphManagerTest, DeleteTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, DeleteResourceNode);
  FRIEND_TEST(FlowGraphManagerTest, UnschedAggCapacityAdjustment);
  FRIEND_TEST(FlowGraphManagerTest, DeleteReAddResourceTopo);
  FRIEND_TEST(FlowGraphManagerTest, DeleteReAddResourceTopoAndJob);
  void AddArcsForTask(FlowGraphNode* task_node, FlowGraphNode* unsched_agg_node,
                      vector<FlowGraphArc*>* task_arcs);
  void AddArcFromParentToResource(const FlowGraphNode& res_node,
                                  ResourceID_t parent_res_id,
                                  vector<FlowGraphArc*>* arcs);
  void AddArcsFromToOtherEquivNodes(EquivClass_t equiv_class,
                                    FlowGraphNode* ec_node);
  FlowGraphNode* AddEquivClassNode(EquivClass_t ec);
  FlowGraphNode* AddNewResourceNode(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddOrUpdateEquivClassArcs(EquivClass_t ec,
                                 vector<FlowGraphArc*>* ec_arcs);
  FlowGraphNode* AddOrUpdateJobUnscheduledAgg(JobID_t job_id);
  void AddResourceEquivClasses(FlowGraphNode* res_node);
  void AddOrUpdateResourceNode(ResourceTopologyNodeDescriptor* rtnd);
  void AddSpecialNodes();
  void AddTaskEquivClasses(FlowGraphNode* task_node);
  FlowGraphNode* AddTaskNode(JobDescriptor* jd_ptr, TaskDescriptor* td_ptr,
                             FlowGraphNode* unsched_agg_node);
  uint64_t CapacityBetweenECNodes(const FlowGraphNode& src,
                                  const FlowGraphNode& dst);
  void ConfigureResourceNodeECs(ResourceTopologyNodeDescriptor* rtnd);
  void ConfigureResourceBranchNode(const ResourceTopologyNodeDescriptor& rtnd,
                                   FlowGraphNode* new_node);
  void ConfigureResourceLeafNode(const ResourceTopologyNodeDescriptor& rtnd,
                                 FlowGraphNode* new_node);
  uint32_t CountTaskSlotsBelowResourceNode(FlowGraphNode* node);
  void DeleteResourceNode(FlowGraphNode* res_node, const char *comment = NULL);
  uint64_t DeleteTaskNode(TaskID_t task_id, const char *comment = NULL);
  void DeleteOrUpdateIncomingEquivNode(EquivClass_t task_equiv,
                                       const char *comment = NULL);
  void DeleteOrUpdateOutgoingEquivNode(EquivClass_t task_equiv,
                                       const char *comment = NULL);
  void PinTaskToNode(FlowGraphNode* task_node, FlowGraphNode* res_node);
  void RemoveInvalidPreferenceArcs(const FlowGraphNode& ec_node,
                                   const vector<ResourceID_t>& res_pref_arcs);
  void RemoveMachineSubTree(FlowGraphNode* res_node,
                            set<uint64_t>* pus_removed);
  void SetResourceNodeType(FlowGraphNode* res_node,
                           const ResourceDescriptor& rd);
  void UpdateArcsForBoundTask(TaskID_t tid, ResourceID_t res_id);
  void UpdateArcsForEvictedTask(TaskID_t task_id, ResourceID_t res_id);
  void UpdateResourceNode(ResourceTopologyNodeDescriptor* rtnd);
  void UpdateUnscheduledAggToSinkCapacity(JobID_t job, int64_t delta);

  // Flow scheduling cost model used
  CostModelInterface* cost_model_;

  FlowGraph* flow_graph_;

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

  // Mapping storing flow graph nodes for each task equivalence class.
  unordered_map<EquivClass_t, FlowGraphNode*> tec_to_node_;

  // Vector storing the graph changes occured since the last scheduling round.
  vector<DIMACSChange*> graph_changes_;
  GenerateTrace* generate_trace_;
  DIMACSChangeStats* dimacs_stats_;
  uint32_t cur_traversal_mark_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H
