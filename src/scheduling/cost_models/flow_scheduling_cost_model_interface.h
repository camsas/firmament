// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Abstract class representing the interface for cost model implementations.

#ifndef FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H

#include <string>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/types.h"
#include "scheduling/common.h"

namespace firmament {

typedef int64_t Cost_t;

// List of cost models supported
enum FlowSchedulingCostModelType {
  COST_MODEL_TRIVIAL = 0,
  COST_MODEL_RANDOM = 1,
  COST_MODEL_SJF = 2,
  COST_MODEL_QUINCY = 3,
  COST_MODEL_WHARE = 4,
  COST_MODEL_COCO = 5,
  COST_MODEL_OCTOPUS = 6,
  COST_MODEL_VOID = 7,
};

class FlowSchedulingCostModelInterface {
 public:
  FlowSchedulingCostModelInterface() {}
  virtual ~FlowSchedulingCostModelInterface() {}

  /**
   * Get the cost from a task node to its unscheduled aggregator node.
   * The method should return a monotonically increasing value upon subsequent
   * calls. It is used to adjust the cost of leaving a task unscheduled after
   * each iteration.
   */
  virtual Cost_t TaskToUnscheduledAggCost(TaskID_t task_id) = 0;
  virtual Cost_t UnscheduledAggToSinkCost(JobID_t job_id) = 0;

  /**
   * Get the cost of an arc from a task node to the cluster
   * aggregator node.
   */
  virtual Cost_t TaskToClusterAggCost(TaskID_t task_id) = 0;

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
  virtual Cost_t EquivClassToResourceNode(EquivClass_t tec,
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
   * Called by the flow_graph when a machine is removed.
   */
  virtual void RemoveMachine(ResourceID_t res_id) = 0;

  virtual void RemoveTask(TaskID_t task_id) = 0;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_SCHEDULING_COST_MODEL_H
