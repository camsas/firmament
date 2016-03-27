// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#ifndef FIRMAMENT_SCHEDULING_QUINCY_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_QUINCY_COST_MODEL_H

#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/time_interface.h"
#include "misc/trace_generator.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/knowledge_base.h"

DECLARE_bool(preemption);

namespace firmament {

class QuincyCostModel : public CostModelInterface {
 public:
  QuincyCostModel(shared_ptr<ResourceMap_t> resource_map,
                  shared_ptr<JobMap_t> job_map,
                  shared_ptr<TaskMap_t> task_map,
                  shared_ptr<KnowledgeBase> knowledge_base,
                  TraceGenerator* trace_generator);
  ~QuincyCostModel();

  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
  Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                  ResourceID_t resource_id);
  // Costs within the resource topology
  Cost_t ResourceNodeToResourceNodeCost(const ResourceDescriptor& source,
                                        const ResourceDescriptor& destination);
  Cost_t LeafResourceNodeToSinkCost(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  Cost_t TaskContinuationCost(TaskID_t task_id);
  Cost_t TaskPreemptionCost(TaskID_t task_id);
  // Costs to equivalence class aggregators
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  pair<Cost_t, uint64_t> EquivClassToResourceNode(
      EquivClass_t tec,
      ResourceID_t res_id);
  pair<Cost_t, uint64_t> EquivClassToEquivClass(EquivClass_t tec1,
                                                EquivClass_t tec2);
  // Get the type of equiv class.
  vector<EquivClass_t>* GetEquivClassToEquivClassesArcs(EquivClass_t tec);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(EquivClass_t tec);
  vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void AddTask(TaskID_t task_id);
  void RemoveMachine(ResourceID_t res_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  void PrepareStats(FlowGraphNode* accumulator);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  uint64_t ComputeClusterDataStatistics(
      const TaskDescriptor& td,
      unordered_map<ResourceID_t, uint64_t,
        boost::hash<boost::uuids::uuid>>* data_on_machines,
      unordered_map<EquivClass_t, uint64_t>* data_on_racks);
  /**
   * Compute the amount of data the task has on the machine given as argument.
   * @param td the descriptor of the task for which to compute statistics
   * @param machine_res_id resource id of the machine we're computing stats for
   * @param data_on_rack the amount of unique data the task has on the
   * machine's rack
   * @param data_on_machine the amount of unique data the task has on the
   * machine
   * @return the total task input size
   */
  uint64_t ComputeDataStatsForMachine(
      const TaskDescriptor& td, ResourceID_t machine_res_id,
      uint64_t* data_on_rack, uint64_t* data_on_machine);
  int64_t ComputeTransferCostToMachine(uint64_t remote_data,
                                       uint64_t data_on_rack);
  int64_t ComputeTransferCostToRack(
      EquivClass_t ec,
      uint64_t input_size,
      uint64_t data_on_rack,
      const unordered_map<ResourceID_t, uint64_t,
        boost::hash<boost::uuids::uuid>>& data_on_machines);
  void ConstructTaskPreferredSet(TaskID_t task_id);
  /**
   * Get the transfer cost to a resource that is not preferred and on which
   * the task is currently running.
   * @param task_id the id of the task
   * @param res_id the id of the resource on which the task is running
   */
  uint64_t GetTransferCostToNotPreferredRes(TaskID_t task_id,
                                            ResourceID_t res_id);
  void RemoveMachineFromRack(ResourceID_t res_id, EquivClass_t rack_ec,
                             bool* rack_removed);
  void RemovePreferencesToMachine(ResourceID_t res_id);
  void RemovePreferencesToRack(EquivClass_t ec);
  void UpdateMachineBlocks(
      const DataLocation& location,
      unordered_map<ResourceID_t, unordered_map<uint64_t, uint64_t>,
        boost::hash<boost::uuids::uuid>>* data_on_machines);
  void UpdateRackBlocks(
      const DataLocation& location,
      unordered_map<EquivClass_t,
        unordered_map<uint64_t, uint64_t>>* data_on_racks);
  /**
   * Updates a task's cached transfer costs that have been affected by a
   * change (e.g., machine addition or removal) in a rack.
   * @param td the descriptor of the task
   * @param ec_changed the rack in which the machines changed
   * @param rack_removed true if the rack has already been removed
   */
  void UpdateTaskCosts(const TaskDescriptor& td, EquivClass_t ec_changed,
                       bool rack_removed);
  int64_t UpdateTaskCostForRack(const TaskDescriptor& td, EquivClass_t rack_ec);

  /**
   * Depending on how much data the machine has, the method adds, updates
   * or removes the machine from the preferred set.
   * @param input_size the total input of the task
   * @param machine_res_id the resource descriptor of the machine
   * @param data_on_machine the amount of task's data (in bytes) the machine
   * stores
   * @param transfer_cost the cost to transfer the non-local data
   * @param task_pref_machines the map storing the task's machine preferences
   * @return an updated version of the task's machine preference map. If the
   * passed task_pref_machines is NULL the method may return a new pointer
   */
  unordered_map<ResourceID_t, int64_t, boost::hash<boost::uuids::uuid>>*
    UpdateTaskPreferredMachineList(
      TaskID_t task_id,
      uint64_t input_size,
      ResourceID_t machine_res_id,
      uint64_t data_on_machine,
      int64_t transfer_cost,
      unordered_map<ResourceID_t, int64_t, boost::hash<boost::uuids::uuid>>*
      task_pref_machines);
  void UpdateTaskPreferredRacksList(
      TaskID_t task_id, uint64_t input_size, uint64_t data_on_rack,
      int64_t worst_rack_cost, EquivClass_t rack_ec);
  inline uint64_t GetNumSchedulableSlots(ResourceID_t res_id) {
    ResourceStatus* rs = FindPtrOrNull(*resource_map_, res_id);
    CHECK_NOTNULL(rs);
    if (FLAGS_preemption) {
      return rs->descriptor().num_slots_below();
    } else {
      return rs->descriptor().num_slots_below() -
        rs->descriptor().num_running_tasks_below();
    }
  }
  inline const TaskDescriptor& GetTask(TaskID_t task_id) {
    TaskDescriptor* td = FindPtrOrNull(*task_map_, task_id);
    CHECK_NOTNULL(td);
    return *td;
  }

  // Lookup maps for various resources from the scheduler.
  shared_ptr<ResourceMap_t> resource_map_;
  // Information regarding jobs and tasks.
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<TaskMap_t> task_map_;
  // A knowledge base instance that we will refer to for job runtime statistics.
  shared_ptr<KnowledgeBase> knowledge_base_;
  // EC corresponding to the cluster aggregator node.
  EquivClass_t cluster_aggregator_ec_;
  // Map storing the machine resource ids associated with each rack.
  unordered_map<EquivClass_t,
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>>
    rack_to_machine_res_;
  // Set storing the racks to which we can still connect machines.
  unordered_set<EquivClass_t> racks_with_spare_links_;
  // Map storing the rack EC associated with each machine.
  unordered_map<ResourceID_t, EquivClass_t, boost::hash<boost::uuids::uuid>>
    machine_to_rack_ec_;
  // Map storing the EC preference list for each task.
  unordered_map<TaskID_t, unordered_map<EquivClass_t, int64_t>>
    task_preferred_ecs_;
  // Map storing the machine preference list for each task.
  unordered_map<TaskID_t,
    unordered_map<ResourceID_t, int64_t, boost::hash<boost::uuids::uuid>>>
    task_preferred_machines_;
  // Map storing the data transfer cost and the resource for each running task.
  unordered_map<TaskID_t, pair<ResourceID_t, int64_t>> task_running_arcs_;
  TraceGenerator* trace_generator_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_QUINCY_COST_MODEL_H
