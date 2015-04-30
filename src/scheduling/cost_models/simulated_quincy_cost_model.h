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
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/cost_models/google_runtime_distribution.h"
#include "scheduling/cost_models/simulated_dfs.h"
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"
#include "scheduling/knowledge_base.h"

namespace firmament {

typedef int64_t Cost_t;

class SimulatedQuincyCostModel : public FlowSchedulingCostModelInterface {
 public:
  SimulatedQuincyCostModel(shared_ptr<ResourceMap_t> resource_map,
                  shared_ptr<JobMap_t> job_map,
                  shared_ptr<TaskMap_t> task_map,
                  unordered_map<TaskID_t, ResourceID_t> *task_bindings,
                  unordered_set<ResourceID_t,
                    boost::hash<boost::uuids::uuid>>* leaf_res_ids,
                  KnowledgeBase* kb,
									double delta_preferred_machine,
									double delta_preferred_rack,
									Cost_t core_transfer_cost,
									Cost_t tor_transfer_cost,
									uint32_t percent_block_tolerance,
									uint64_t machines_per_rack,
									SimulatedDFS *dfs);

  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
  Cost_t TaskToClusterAggCost(TaskID_t task_id);
  Cost_t TaskToResourceNodeCost(TaskID_t task_id,
                                ResourceID_t resource_id);
  // Costs within the resource topology
  Cost_t ResourceNodeToResourceNodeCost(ResourceID_t source,
                                        ResourceID_t destination);
  Cost_t LeafResourceNodeToSinkCost(ResourceID_t resource_id);
  // Costs pertaining to preemption (i.e. already running tasks)
  Cost_t TaskContinuationCost(TaskID_t task_id);
  Cost_t TaskPreemptionCost(TaskID_t task_id);
  // Costs to equivalence class aggregators
  Cost_t TaskToEquivClassAggregator(TaskID_t task_id, EquivClass_t tec);
  Cost_t EquivClassToResourceNode(EquivClass_t tec, ResourceID_t res_id);
  Cost_t EquivClassToEquivClass(EquivClass_t tec1, EquivClass_t tec2);
  // Get the type of equiv class.
  vector<EquivClass_t>* GetTaskEquivClasses(TaskID_t task_id);
  vector<EquivClass_t>* GetResourceEquivClasses(ResourceID_t res_id);
  vector<ResourceID_t>* GetOutgoingEquivClassPrefArcs(EquivClass_t tec);
  vector<TaskID_t>* GetIncomingEquivClassPrefArcs(EquivClass_t tec);
  vector<ResourceID_t>* GetTaskPreferenceArcs(TaskID_t task_id);
  pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    GetEquivClassToEquivClassesArcs(EquivClass_t tec);
  void AddMachine(ResourceTopologyNodeDescriptor* rtnd_ptr);
  void RemoveMachine(ResourceID_t res_id);
  // N.B. Not in generic cost model interface
  void AddTask(TaskID_t task_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  void BuildTaskFileSet(TaskID_t task_id);
  void ComputeCostsAndPreferredSet(TaskID_t task_id);

  // Lookup maps for various resources from the scheduler.
  shared_ptr<ResourceMap_t> resource_map_;
  // Information regarding jobs and tasks.
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<TaskMap_t> task_map_;
  unordered_map<TaskID_t, ResourceID_t>* task_bindings_;
  unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>* leaf_res_ids_;
  // A knowledge base instance that we will refer to for job runtime statistics.
  KnowledgeBase* knowledge_base_;
  uint32_t rand_seed_ = 0;

  double proportion_machine_preferred_, proportion_rack_preferred_;
  Cost_t core_transfer_cost_, tor_transfer_cost_;
  uint32_t percent_block_tolerance_;

  uint64_t machines_per_rack_;
	unordered_map<ResourceID_t, EquivClass_t> machine_to_rack_map_;
	vector<std::list<ResourceID_t>> rack_to_machine_map_;

  SimulatedDFS *filesystem_;
  unordered_map<TaskID_t, std::unordered_set<SimulatedDFS::FileID_t>> file_map_;

  GoogleRuntimeDistribution runtime_distribution_;
  GoogleBlockDistribution block_distribution_;

  unordered_map<TaskID_t, unordered_map<ResourceID_t, Cost_t>>
	                                                       preferred_machine_map_;
  unordered_map<TaskID_t, unordered_map<EquivClass_t, Cost_t>>
	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 preferred_rack_map_;
  unordered_map<TaskID_t, Cost_t> cluster_aggregator_cost_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_QUINCY_COST_MODEL_H
