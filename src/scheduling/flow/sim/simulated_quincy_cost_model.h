// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_COST_MODEL_H
#define FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_COST_MODEL_H

#include <list>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/types.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/knowledge_base.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/sim/google_runtime_distribution.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"

namespace firmament {
namespace scheduler {

typedef unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid> >
        ResourceSet_t;
typedef unordered_map<ResourceID_t, uint64_t, boost::hash<boost::uuids::uuid> >
        ResourceFrequencyMap_t;
typedef unordered_map<ResourceID_t, EquivClass_t,
        boost::hash<boost::uuids::uuid> > ResourceEquivClassMap_t;
typedef unordered_map<ResourceID_t, int64_t, boost::hash<boost::uuids::uuid> >
        ResourceCostMap_t;

using sim::dfs::GoogleBlockDistribution;
using sim::dfs::SimulatedDFS;

class SimulatedQuincyCostModel : public CostModelInterface {
 public:
  SimulatedQuincyCostModel(
      shared_ptr<ResourceMap_t> resource_map, shared_ptr<JobMap_t> job_map,
      shared_ptr<TaskMap_t> task_map,
      unordered_set<ResourceID_t,
                    boost::hash<boost::uuids::uuid>>* leaf_res_ids,
      shared_ptr<KnowledgeBase> knowledge_base, SimulatedDFS* dfs,
      GoogleRuntimeDistribution *runtime_distribution,
      GoogleBlockDistribution *block_distribution,
      double delta_preferred_machine, double delta_preferred_rack,
      Cost_t core_transfer_cost, Cost_t tor_transfer_cost,
      uint32_t percent_block_tolerance, uint64_t machines_per_rack);
  ~SimulatedQuincyCostModel();

  // Costs pertaining to leaving tasks unscheduled
  Cost_t TaskToUnscheduledAggCost(TaskID_t task_id);
  Cost_t UnscheduledAggToSinkCost(JobID_t job_id);
  // Per-task costs (into the resource topology)
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
  pair<Cost_t, int64_t> EquivClassToResourceNode(
      EquivClass_t tec,
      ResourceID_t res_id);
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
  void AddTask(TaskID_t task_id);
  void RemoveTask(TaskID_t task_id);
  FlowGraphNode* GatherStats(FlowGraphNode* accumulator, FlowGraphNode* other);
  FlowGraphNode* UpdateStats(FlowGraphNode* accumulator, FlowGraphNode* other);

 private:
  void BuildTaskFileSet(TaskID_t task_id);
  void ComputeCostsAndPreferredSet(TaskID_t task_id);
  Cost_t TaskToClusterAggCost(TaskID_t task_id);

  // Lookup maps for various resources from the scheduler.
  shared_ptr<ResourceMap_t> resource_map_;
  // Information regarding jobs and tasks.
  shared_ptr<JobMap_t> job_map_;
  shared_ptr<TaskMap_t> task_map_;
  // A knowledge base instance that we will refer to for job runtime statistics.
  shared_ptr<KnowledgeBase> knowledge_base_;

  double proportion_machine_preferred_;
  double proportion_rack_preferred_;
  Cost_t core_transfer_cost_;
  Cost_t tor_transfer_cost_;
  uint32_t percent_block_tolerance_;

  uint64_t machines_per_rack_;
  ResourceEquivClassMap_t machine_to_rack_map_;
  vector<list<ResourceID_t>> rack_to_machine_map_;

  SimulatedDFS *filesystem_;
  unordered_map<TaskID_t, unordered_set<SimulatedDFS::FileID_t>> file_map_;

  GoogleRuntimeDistribution *runtime_distribution_;
  GoogleBlockDistribution *block_distribution_;

  unordered_map<TaskID_t, ResourceCostMap_t*> preferred_machine_map_;
  unordered_map<TaskID_t, unordered_map<EquivClass_t, Cost_t>>
    preferred_rack_map_;
  unordered_map<TaskID_t, Cost_t> cluster_aggregator_cost_;
};

}  // namespace scheduler
}  // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_COST_MODEL_H
