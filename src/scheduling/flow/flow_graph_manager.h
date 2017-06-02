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

#ifndef FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H
#define FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H

#include <queue>
#include <set>
#include <string>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "misc/map-util.h"
#include "misc/time_interface.h"
#include "misc/trace_generator.h"
#include "scheduling/scheduling_delta.pb.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/dimacs_change.h"
#include "scheduling/flow/dimacs_change_stats.h"
#include "scheduling/flow/flow_graph_arc.h"
#include "scheduling/flow/flow_graph_change_manager.h"
#include "scheduling/flow/flow_graph_node.h"

DECLARE_bool(preemption);
DECLARE_string(flow_scheduling_solver);

namespace firmament {

struct TDOrNodeWrapper {
  TDOrNodeWrapper(TaskDescriptor* td_ptr) : node_(NULL), td_ptr_(td_ptr) {
  }
  TDOrNodeWrapper(FlowGraphNode* node,
                  TaskDescriptor* td_ptr) : node_(node), td_ptr_(td_ptr) {
  }
  FlowGraphNode* node_;
  TaskDescriptor* td_ptr_;
};

class FlowGraphManager {
 public:
  explicit FlowGraphManager(CostModelInterface* cost_model,
                            unordered_set<ResourceID_t,
                              boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                            TimeInterface* time_manager,
                            TraceGenerator* trace_generator,
                            DIMACSChangeStats* dimacs_stats);
  virtual ~FlowGraphManager();
  void AddOrUpdateJobNodes(const vector<JobDescriptor*>& jd_ptr_vect);

  /**
   * Adds the entire resource topology tree rooted at rtnd_ptr. The method
   * also updates the statistics of the nodes up to the root resource.
   * @param rtnd_ptr the topology descriptor of the root resource from which to
   * start adding nodes
   */
  void AddResourceTopology(ResourceTopologyNodeDescriptor* rtnd_ptr);

  void ComputeTopologyStatistics(
      FlowGraphNode* node,
      boost::function<void(FlowGraphNode*)> prepare,
      boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> gather,
      boost::function<FlowGraphNode*(FlowGraphNode*, FlowGraphNode*)> update);
  void JobCompleted(JobID_t job_id);
  void JobRemoved(JobID_t job_id);
  void NodeBindingToSchedulingDeltas(
      uint64_t task_node_id, uint64_t resource_node_id,
      unordered_map<TaskID_t, ResourceID_t>* task_bindings,
      vector<SchedulingDelta*>* deltas);

  /**
   * As a result of task state change, preferences change or
   * resource removal we may end up with unconnected equivalence
   * class nodes. This method makes sure they are removed.
   * We cannot end up with unconnected unscheduled agg nodes,
   * task or resource nodes.
   */
  void PurgeUnconnectedEquivClassNodes();

  /**
   * Removes the entire resource topology tree rooted at rd. The method also
   * updates the statistics of the nodes up to the root resource.
   * @param rd the descriptor of the root resource from which to start removing
   * nodes
   * @param pus_removed set to which to append the IDs of the removed PUs
   */
  void RemoveResourceTopology(const ResourceDescriptor& rd,
                              set<uint64_t>* pus_removed);
  void SchedulingDeltasForPreemptedTasks(
      const multimap<uint64_t, uint64_t>& task_mappings,
      shared_ptr<ResourceMap_t> resource_map,
      vector<SchedulingDelta*>* deltas);
  uint64_t TaskCompleted(TaskID_t task_id);
  void TaskEvicted(TaskID_t task_id, ResourceID_t res_id);
  void TaskFailed(TaskID_t task_id);
  void TaskKilled(TaskID_t task_id);
  void TaskMigrated(TaskID_t task_id,
                    ResourceID_t old_res_id,
                    ResourceID_t new_res_id);
  void TaskRemoved(TaskID_t task_id);
  void TaskScheduled(TaskID_t task_id, ResourceID_t res_id);

  /**
   * Update each task's arc to its unscheduled aggregator. Moreover, for
   * running tasks we update their continuation costs.
   */
  void UpdateAllCostsToUnscheduledAggs();

  void UpdateResourceTopology(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void UpdateTimeDependentCosts(const vector<JobDescriptor*>& jd_ptr_vec);

  // Simple accessor methods
  inline FlowGraphChangeManager* flow_graph_change_manager() {
    return graph_change_manager_;
  }
  inline const unordered_set<uint64_t>& leaf_node_ids() const {
    return leaf_nodes_;
  }
  inline FlowGraphNode* sink_node() {
    return sink_node_;
  }

 private:
  FRIEND_TEST(DIMACSExporterTest, LargeGraph);
  FRIEND_TEST(DIMACSExporterTest, ScalabilityTestGraphs);
  FRIEND_TEST(DIMACSExporterTest, SimpleGraphOutput);
  FRIEND_TEST(FlowGraphManagerTest, AddEquivClassNode);
  FRIEND_TEST(FlowGraphManagerTest, AddResourceNode);
  FRIEND_TEST(FlowGraphManagerTest, AddResourceTopologyDFS);
  FRIEND_TEST(FlowGraphManagerTest, AddTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, AddUnscheduledAggNode);
  FRIEND_TEST(FlowGraphManagerTest, PinTaskToNode);
  FRIEND_TEST(FlowGraphManagerTest, PurgeUnconnectedEquivClassNodes);
  FRIEND_TEST(FlowGraphManagerTest, RemoveEquivClassNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveInvalidECPrefArcs);
  FRIEND_TEST(FlowGraphManagerTest, RemoveInvalidPrefResArcs);
  FRIEND_TEST(FlowGraphManagerTest, RemoveResourceNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveTaskHelper);
  FRIEND_TEST(FlowGraphManagerTest, TaskScheduled);
  FRIEND_TEST(FlowGraphManagerTest, TraverseAndRemoveTopology);
  FRIEND_TEST(FlowGraphManagerTest, UpdateAllCostsToUnscheduledAggs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateArcsForScheduledTask);
  FRIEND_TEST(FlowGraphManagerTest, UpdateChildrenTasks);
  FRIEND_TEST(FlowGraphManagerTest, UpdateEquivClassNode);
  FRIEND_TEST(FlowGraphManagerTest, UpdateEquivToEquivArcs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateEquivToResArcs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateFlowGraph);
  FRIEND_TEST(FlowGraphManagerTest, UpdateResourceStatsUpToRoot);
  FRIEND_TEST(FlowGraphManagerTest, UpdateResOutgoingArcs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateResToSinkArc);
  FRIEND_TEST(FlowGraphManagerTest, UpdateRunningTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, UpdateRunningTaskToUnscheduledAggArc);
  FRIEND_TEST(FlowGraphManagerTest, RemoveTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, RemoveUnscheduledAggNode);
  FRIEND_TEST(FlowGraphManagerTest, UpdateTaskNode);
  FRIEND_TEST(FlowGraphManagerTest, UpdateTaskToEquivArcs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateTaskToResArcs);
  FRIEND_TEST(FlowGraphManagerTest, UpdateTaskToUnscheduledAggArc);
  FRIEND_TEST(FlowGraphManagerTest, UpdateUnscheduledAggNode);

  FlowGraphNode* AddEquivClassNode(EquivClass_t ec);
  FlowGraphNode* AddResourceNode(ResourceDescriptor* rd_ptr);

  /**
   * Adds to the graph all the node from the subtree rooted at rtnd_ptr.
   * The method also correctly computes statistics for every new node (e.g.,
   * num slots, num running tasks)
   * @param rtnd_ptr the topology descriptor of the root node
   */
  void AddResourceTopologyDFS(ResourceTopologyNodeDescriptor* rtnd_ptr);

  FlowGraphNode* AddTaskNode(JobID_t job_id, TaskDescriptor* td_ptr);
  FlowGraphNode* AddUnscheduledAggNode(JobID_t job_id);
  void PinTaskToNode(FlowGraphNode* task_node, FlowGraphNode* res_node);
  void RemoveEquivClassNode(FlowGraphNode* ec_node);

  /**
   * Remove invalid preference arcs from node to equivalence class nodes.
   * @param node the node for which to remove its invalid peference arcs
   * to equivalence classes
   * @param pref_ecs node's current preferred equivalence classes
   * @param change_type the type of the change
   */
  void RemoveInvalidECPrefArcs(const FlowGraphNode& node,
                               const vector<EquivClass_t>& pref_ecs,
                               DIMACSChangeType change_type);

  /**
   * Remove invalid preference arcs from node to resource nodes.
   * @param node the node for which to remove its invalid preference arcs to
   * resources
   * @param pref_resources node's current preferred resources
   * @param change_type the type of the change
   */
  void RemoveInvalidPrefResArcs(const FlowGraphNode& node,
                                const vector<ResourceID_t>& pref_resources,
                                DIMACSChangeType change_type);
  void RemoveResourceNode(FlowGraphNode* res_node);
  void RemoveTaskHelper(TaskID_t task_id);
  uint64_t RemoveTaskNode(FlowGraphNode* task_node);
  void RemoveUnscheduledAggNode(JobID_t job_id);

  /**
   * Remove the resource topology rooted at res_node.
   * @param res_node the root of the topology tree to remove
   * @param pus_removed set that gets updated whenever we remove a PU
   */
  void TraverseAndRemoveTopology(FlowGraphNode* res_node,
                                 set<uint64_t>* pus_removed);

  /**
   * Updates the arc of a newly scheduled task.
   * If we're running with preemption enabled then the method just adds/changes
   * an arc to the resource node and updates the arc to the unscheduled agg to
   * have the premeption cost.
   * If we're not running with preemption enabled then the method deletes the
   * task's arcs and only adds a running arc.
   * @param task_node the node of the task recently scheduled
   * @param res_node the node of the resource to which the task has been
   * scheduled
   */
  void UpdateArcsForScheduledTask(FlowGraphNode* task_node,
                                  FlowGraphNode* res_node);

  /**
   * Adds the children tasks of the nodeless current task to the node queue.
   * If a child task doesn't need to have a graph node (e.g., task is not
   * RUNNABLE, RUNNING or ASSIGNED) then its TDOrNodeWrapper will only contain
   * a pointer to its task descriptor.
   */
  void UpdateChildrenTasks(TaskDescriptor* td_ptr,
                           queue<TDOrNodeWrapper*>* node_queue,
                           unordered_set<uint64_t>* marked_nodes);

  void UpdateEquivClassNode(FlowGraphNode* ec_node,
                            queue<TDOrNodeWrapper*>* node_queue,
                            unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates an EC's outgoing arcs to other ECs. If the EC has new outgoing arcs
   * to new EC nodes then the method appends them to the node_queue. Similarly,
   * EC nodes that have not yet been marked are appended to the queue.
   */
  void UpdateEquivToEquivArcs(FlowGraphNode* ec_node,
                              queue<TDOrNodeWrapper*>* node_queue,
                              unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates the resource preference arcs an equivalence class has.
   * @param ec_node node for which to update its preferences
   */
  void UpdateEquivToResArcs(FlowGraphNode* ec_node,
                            queue<TDOrNodeWrapper*>* node_queue,
                            unordered_set<uint64_t>* marked_nodes);

  void UpdateFlowGraph(queue<TDOrNodeWrapper*>* node_queue,
                       unordered_set<uint64_t>* marked_nodes);

  void UpdateResourceNode(FlowGraphNode* res_node,
                          queue<TDOrNodeWrapper*>* node_queue,
                          unordered_set<uint64_t>* marked_nodes);

  /**
   * Update resource related stats (e.g., arc capacities, num slots,
   * num running tasks) on every arc/node up to the root resource.
   */
  void UpdateResourceStatsUpToRoot(FlowGraphNode* cur_node,
                                   int64_t cap_delta,
                                   int64_t slots_delta,
                                   int64_t running_tasks_delta);

  void UpdateResourceTopologyDFS(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void UpdateResOutgoingArcs(FlowGraphNode* res_node,
                             queue<TDOrNodeWrapper*>* node_queue,
                             unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates the arc connecting a resource to the sink. It requires the resource
   * to be a PU.
   * @param res_node the resource node for which to update its arc to the sink
   */
  void UpdateResToSinkArc(FlowGraphNode* res_node);

  /**
   * Updates the cost on running arc of the task. If preemption is enabled then
   * the method also updates the preemption cost on the arc to the unscheduled
   * aggregator.
   * NOTE: node_queue and marked_nodes can be NULL as long as update_preferences
   * is false.
   * @param task_node the node for which to update the arcs
   * @param update_preferences true if the method should update the resource and
   * equivalence preferences
   */
  void UpdateRunningTaskNode(FlowGraphNode* task_node,
                             bool update_preferences,
                             queue<TDOrNodeWrapper*>* node_queue,
                             unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates the cost of the arc connecting a running task with its unscheduled
   * aggregator.
   * NOTE: This method should only be called when preemption is enabled.
   * @param task_node the node for which to update the arc
   */
  void UpdateRunningTaskToUnscheduledAggArc(FlowGraphNode* task_node);

  void UpdateTaskNode(FlowGraphNode* task_node,
                      queue<TDOrNodeWrapper*>* node_queue,
                      unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates a task's outgoing arcs to ECs. If the task has new outgoing arcs
   * to new EC nodes then the method appends them to the node_queue. Similarly,
   * EC nodes that have not yet been marked are appended to the queue.
   */
  void UpdateTaskToEquivArcs(FlowGraphNode* task_node,
                             queue<TDOrNodeWrapper*>* node_queue,
                             unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates a task's preferences to resources.
   * @param task_node node for which to update its preferences
   */
  void UpdateTaskToResArcs(FlowGraphNode* task_node,
                           queue<TDOrNodeWrapper*>* node_queue,
                           unordered_set<uint64_t>* marked_nodes);

  /**
   * Updates the arc from a task to its unscheduled aggregator. The method
   * adds the unscheduled if it doesn't already exist.
   * @param task_node the node for which to update the arc
   * @return the unscheduled aggregator node
   */
  FlowGraphNode* UpdateTaskToUnscheduledAggArc(FlowGraphNode* task_node);

  /**
   * Adjusts the capacity of the arc connecting the unscheduled agg to the sink
   * by cap_delta. The method also updates the cost if need be.
   * @param unsched_agg_node the unscheduled aggregator node
   * @param cap_delta the delta by which to change the capacity
   */
  void UpdateUnscheduledAggNode(FlowGraphNode* unsched_agg_node,
                                int64_t cap_delta);

  void VisitTopologyChildren(ResourceTopologyNodeDescriptor* rtnd_ptr);

  inline FlowGraphNode* NodeForEquivClass(const EquivClass_t& ec) {
    return FindPtrOrNull(tec_to_node_map_, ec);
  }
  inline FlowGraphNode* NodeForResourceID(const ResourceID_t& res_id) {
    return FindPtrOrNull(resource_to_node_map_, res_id);
  }
  inline FlowGraphNode* NodeForTaskID(TaskID_t task_id) {
    return FindPtrOrNull(task_to_node_map_, task_id);
  }
  inline bool TaskMustHaveNode(const TaskDescriptor& td) {
    return td.state() == TaskDescriptor::RUNNABLE ||
      td.state() == TaskDescriptor::RUNNING ||
      td.state() == TaskDescriptor::ASSIGNED;
  }
  inline FlowGraphNode* UnschedAggNodeForJobID(JobID_t job_id) {
    return FindPtrOrNull(job_unsched_to_node_, job_id);
  }

  // Resource and task mappings
  unordered_map<TaskID_t, FlowGraphNode*> task_to_node_map_;
  unordered_map<ResourceID_t, FlowGraphNode*,
      boost::hash<boost::uuids::uuid> > resource_to_node_map_;
  // Mapping storing flow graph node for each task equivalence class.
  unordered_map<EquivClass_t, FlowGraphNode*> tec_to_node_map_;
  // Mapping storing flow graph node for each unscheduled aggregator.
  unordered_map<JobID_t, FlowGraphNode*,
      boost::hash<boost::uuids::uuid> > job_unsched_to_node_;

  // The "node ID" for the job is currently the ID of the job's unscheduled node
  unordered_set<uint64_t> leaf_nodes_;
  // Map storing the running arc for every task that is running.
  unordered_map<TaskID_t, FlowGraphArc*> task_to_running_arc_;
  unordered_map<FlowGraphNode*, FlowGraphNode*> node_to_parent_node_map_;
  FlowGraphNode* sink_node_;
  CostModelInterface* cost_model_;
  FlowGraphChangeManager* graph_change_manager_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  TraceGenerator* trace_generator_;
  DIMACSChangeStats* dimacs_stats_;
  // Counter updated whenever we compute topology statistics. The counter is
  // used as a marker in the resource topology traversal. It helps us to avoid
  // having to reset the visited state before each traversal.
  uint32_t cur_traversal_counter_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_FLOW_GRAPH_MANAGER_H
