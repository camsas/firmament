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

// Coordinated co-location scheduling cost model.

#ifndef FIRMAMENT_SCHEDULING_COCO_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_COCO_COST_MODEL_H

#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/time_interface.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"

namespace firmament {

typedef struct CostVector {
  // record number of dimensions here
  static const int16_t dimensions_ = 8;
  // Data follows
  uint32_t priority_;
  uint32_t cpu_cores_;
  uint32_t ram_cap_;
  uint32_t network_tx_bw_;
  uint32_t network_rx_bw_;
  uint32_t disk_bw_;
  uint32_t machine_type_score_;
  uint32_t interference_score_;
  uint32_t locality_score_;
} CostVector_t;

class CocoCostModel : public CostModelInterface {
 public:
  CocoCostModel(shared_ptr<ResourceMap_t> resource_map,
                const ResourceTopologyNodeDescriptor& resource_topology,
                shared_ptr<TaskMap_t> task_map,
                unordered_set<ResourceID_t,
                  boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                shared_ptr<KnowledgeBase> knowledge_base,
                TimeInterface* time_manager);
  const string DebugInfo() const;
  const string DebugInfoCSV() const;
  // Costs pertaining to leaving tasks unscheduled
  ArcDescriptor TaskToUnscheduledAgg(TaskID_t task_id);
  ArcDescriptor UnscheduledAggToSink(JobID_t job_id);
  // Per-task costs (into the resource topology)
  ArcDescriptor TaskToResourceNode(TaskID_t task_id, ResourceID_t resource_id);
  // Costs within the resource topology
  ArcDescriptor ResourceNodeToResourceNode(
      const ResourceDescriptor& source,
      const ResourceDescriptor& destination);
  ArcDescriptor LeafResourceNodeToSink(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  ArcDescriptor TaskContinuation(TaskID_t task_id);
  ArcDescriptor TaskPreemption(TaskID_t task_id);
  // Costs to equivalence class aggregators
  ArcDescriptor TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  ArcDescriptor EquivClassToResourceNode(EquivClass_t tec, ResourceID_t res_id);
  ArcDescriptor EquivClassToEquivClass(EquivClass_t tec1, EquivClass_t tec2);
  // Get the type of equiv class.
  vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(EquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  vector<EquivClass_t>* GetEquivClassToEquivClassesArcs(EquivClass_t tec);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddTask(TaskID_t task_id);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  void PrepareStats(FlowGraphNode* accumulator);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  // Fixed value for OMEGA, the normalization ceiling for each dimension's cost
  // value
  const Cost_t omega_ = 1000;
  const int64_t MAX_PRIORITY_VALUE = 10LL;

  // Resource vector comparison type and enum
  typedef enum {
    RESOURCE_VECTOR_DOES_NOT_FIT = 0,
    RESOURCE_VECTOR_PARTIALLY_FITS = 1,
    RESOURCE_VECTOR_WHOLLY_FITS = 2,
  } ResourceVectorFitIndication_t;
  // Task fit type and enum
  typedef enum {
    TASK_NEVER_FITS = 0,
    // N.B.: we assumes that unreserved <= available at all times;
    // in other words, reserved > used. Hence, TASK_SOMETIMES_FITS_IN_UNRESERVED
    // also *implies* TASK_SOMETIMES_FITS_IN_AVAILABLE, and
    // TASK_ALWAYS_FITS_IN_UNRESERVED *implies* TASK_ALWAYS_FITS_IN_AVAILABLE.
    TASK_SOMETIMES_FITS_IN_UNRESERVED = 1,
    TASK_SOMETIMES_FITS_IN_AVAILABLE = 2,
    TASK_ALWAYS_FITS_IN_UNRESERVED = 3,
    TASK_ALWAYS_FITS_IN_AVAILABLE = 4,
  } TaskFitIndication_t;

  // Load statistics accumulator helper
  void AccumulateResourceStats(ResourceDescriptor* accumulator,
                               ResourceDescriptor* other);
  // Check if rv1 fits into rv2 fully, partially or not at all.
  ResourceVectorFitIndication_t CompareResourceVectors(
    const ResourceVector& rv1,
    const ResourceVector& rv2);
  // Interference score
  int64_t ComputeInterferenceScore(ResourceID_t res_id);
  // Helper method to get TD for a task ID
  const TaskDescriptor& GetTask(TaskID_t task_id);
  void GetInterferenceScoreForTask(TaskID_t task_id,
                                   CoCoInterferenceScores* interference_vector);
  Cost_t FlattenCostVector(CostVector_t cv);
  Cost_t FlattenInterferenceScore(const CoCoInterferenceScores& iv);
  // Get machine resource for a lower-level resource
  ResourceID_t MachineResIDForResource(ResourceID_t res_id);
  // Bring cost into the range (0, omega_)
  Cost_t NormalizeCost(double raw_cost, double max_cost);
  void PrintCostVector(CostVector_t cv);
  // Get a delimited string representing a resource vector
  string ResourceVectorToString(const ResourceVector& rv,
                                const string& delimiter) const;
  // Count how many times a task with resource request req fits into
  // available resources avail
  uint64_t TaskFitCount(const ResourceVector& req,
                        const ResourceVector& avail);
  // Check if a task resource request fits under a resource aggregate
  TaskFitIndication_t TaskFitsUnderResourceAggregate(
      EquivClass_t tec,
      const ResourceDescriptor& res);
  // Cost to cluster aggregator EC
  Cost_t TaskToClusterAggCost(TaskID_t task_id);

  // Lookup maps for various resources from the scheduler.
  shared_ptr<ResourceMap_t> resource_map_;
  const ResourceTopologyNodeDescriptor& resource_topology_;
  shared_ptr<TaskMap_t> task_map_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  // A knowledge base instance that we will refer to for job runtime statistics.
  shared_ptr<KnowledgeBase> knowledge_base_;

  // Mapping between task equiv classes and connected tasks.
  unordered_map<EquivClass_t, unordered_set<TaskID_t> > task_ec_to_set_task_id_;
  unordered_map<EquivClass_t, ResourceVector> task_ec_to_resource_request_;
  // Track equivalence class aggregators present
  unordered_set<EquivClass_t> task_aggs_;

  // Largest cost seen so far, plus one
  Cost_t infinity_;
  // Vector to track the maximum capacity values in each dimension
  // present in the cluster (N.B.: these can execeed OMEGA).
  ResourceVector max_machine_capacity_;
  ResourceVector min_machine_capacity_;
  TimeInterface* time_manager_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_COCO_COST_MODEL_H
