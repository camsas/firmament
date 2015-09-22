// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Abstract class representing the interface for cost model implementations.

#ifndef FIRMAMENT_SCHEDULING_FLOW_COST_MODEL_INTERFACE_H
#define FIRMAMENT_SCHEDULING_FLOW_COST_MODEL_INTERFACE_H

#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/types.h"
#include "scheduling/common.h"
#include "scheduling/flow/flow_graph_node.h"

namespace firmament {

typedef int64_t Cost_t;

// List of cost models supported
enum CostModelType {
  COST_MODEL_TRIVIAL = 0,
  COST_MODEL_RANDOM = 1,
  COST_MODEL_SJF = 2,
  COST_MODEL_QUINCY = 3,
  COST_MODEL_WHARE = 4,
  COST_MODEL_COCO = 5,
  COST_MODEL_OCTOPUS = 6,
  COST_MODEL_VOID = 7,
  COST_MODEL_SIMULATED_QUINCY = 8,
};

// Forward declarations to avoid cyclic dependencies
class FlowGraphManager;

class CostModelInterface {
 public:
  CostModelInterface() {}
  virtual ~CostModelInterface() {}

  /**
   * Get the cost from a task node to its unscheduled aggregator node.
   * The method should return a monotonically increasing value upon subsequent
   * calls. It is used to adjust the cost of leaving a task unscheduled after
   * each iteration.
   */
  virtual Cost_t TaskToUnscheduledAggCost(TaskID_t task_id) = 0;
  virtual Cost_t UnscheduledAggToSinkCost(JobID_t job_id) = 0;

  /**
   * Get the cost of a preference arc from a task node to a resource node.
   */
  virtual Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                        ResourceID_t resource_id) = 0;

  /**
   * Get the cost of an arc between two resource nodes.
   */
  virtual Cost_t ResourceNodeToResourceNodeCost(ResourceID_t source,
                                                ResourceID_t destination) = 0;
  /**
   * Get the cost of an arc from a resource to the sink.
   **/
  virtual Cost_t LeafResourceNodeToSinkCost(ResourceID_t resource_id) = 0;

  // Costs pertaining to preemption (i.e. already running tasks)
  virtual Cost_t TaskContinuationCost(TaskID_t task_id) = 0;
  virtual Cost_t TaskPreemptionCost(TaskID_t task_id) = 0;

  /**
   * Get the cost of an arc from a task node to an equivalence class node.
   */
  virtual Cost_t TaskToEquivClassAggregator(TaskID_t task_id,
                                            EquivClass_t tec) = 0;
  /**
   * Get the cost of an arc from an equivalence class node to a resource node.
   */
  virtual pair<Cost_t, int64_t> EquivClassToResourceNode(
      EquivClass_t tec,
      ResourceID_t res_id) = 0;
  /**
   * Get the cost of an arc from an equivalence class node to another
   * equivalence class node.
   * @param tec1 the source equivalence class
   * @param tec2 the destination equivalence class
   */
  virtual Cost_t EquivClassToEquivClass(EquivClass_t tec1,
                                        EquivClass_t tec2) = 0;
  /**
   * Get the equivalence classes of a task.
   * @param task_id the task id for which to get the equivalence classes
   * @return a vector containing the task's equivalence classes
   */
  virtual vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id) = 0;

  /**
   * Get the equivalence classes for a resource.
   * @param res_id the resource id for which to get the equivalence classes
   * @return a vector containing the resource's equivalence classes
   */
  virtual vector<EquivClass_t>* GetResourceEquivClasses(
      ResourceID_t res_id) = 0;

  /**
   * Get the resource ids to which an equivalence class has arcs.
   * @param tec the equivalence class for which to get the resource ids
   */
  virtual vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(
      EquivClass_t tec) = 0;

  /**
   * Get the task ids that have preference arcs to the given equivalence class.
   * @param tec the equivalence class for which to get the task ids
   */
  virtual vector<TaskID_t>* GetIncomingEquivClassPrefArcs(
      EquivClass_t tec) = 0;

  /**
   * Get the resource preference arcs of a task.
   * @param task_id the id of the task for which to get the preference arcs
   */
  virtual vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id) = 0;

  /**
   * Get equivlance classes to which an equivalence class is connected.
   * @return a pair consisting of two vectors. The first one contains the
   * equivalence classes from which we have an incoming arc and the second one
   * equivalence classes to which we have an outgoing arc.
   */
  virtual pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    GetEquivClassToEquivClassesArcs(EquivClass_t tec) = 0;

  /**
   * Called by the flow_graph when a machine is added.
   */
  virtual void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr) = 0;

  /**
   * Called by the flow graph when a task is submitted.
   */
  virtual void AddTask(TaskID_t task_id) = 0;

  /**
   * Called by the flow_graph when a machine is removed.
   */
  virtual void RemoveMachine(ResourceID_t res_id) = 0;

  virtual void RemoveTask(TaskID_t task_id) = 0;

  /**
   * Gathers statistics during reverse traversal of resource topology (from
   * sink upwards). Called on pairs of connected nodes.
   */
  virtual FlowGraphNode* GatherStats(FlowGraphNode* accumulator,
                                     FlowGraphNode* other) = 0;

  /**
   * The default Prepare action is a no-op. Cost models can override this if
   * they need to perform preparation actions before GatherStats is invoked.
   */
  virtual void PrepareStats(FlowGraphNode* accumulator) { }

  /**
   * Generates updates for arc costs in the resource topology.
   */
  virtual FlowGraphNode* UpdateStats(FlowGraphNode* accumulator,
                                     FlowGraphNode* other) = 0;

  /**
   * Handle to pull debug information from cost model; return string.
   */
  virtual const string DebugInfo() const {
    // Default no-op implementation;
    return "";
  }
  virtual const string DebugInfoCSV() const {
    // Default no-op implementation;
    return "";
  }

  inline void SetFlowGraphManager(
      shared_ptr<FlowGraphManager> flow_graph_manager) {
    flow_graph_manager_ = flow_graph_manager;
  }

 protected:
  shared_ptr<FlowGraphManager> flow_graph_manager_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_COST_MODEL_INTERFACE_H
