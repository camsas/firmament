// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Quincy scheduling cost model, as described in the SOSP 2009 paper.

#include "scheduling/cost_models/simulated_quincy_cost_model.h"

#include <set>
#include <string>
#include <unordered_map>

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "misc/utils.h"
#include "scheduling/common.h"
#include "scheduling/cost_models/flow_scheduling_cost_model_interface.h"
#include "scheduling/knowledge_base.h"

namespace firmament {

SimulatedQuincyCostModel::SimulatedQuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    unordered_map<TaskID_t, ResourceID_t> *task_bindings,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids,
    KnowledgeBase* kb,
    SimulatedDFS* dfs,
    GoogleRuntimeDistribution *runtime_distribution,
    GoogleBlockDistribution *block_distribution,
		double delta_preferred_machine,
		double delta_preferred_rack,
		Cost_t core_transfer_cost,
		Cost_t tor_transfer_cost,
		uint32_t percent_block_tolerance,
		uint64_t machines_per_rack)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    knowledge_base_(kb),
		proportion_machine_preferred_(delta_preferred_machine),
		proportion_rack_preferred_(delta_preferred_rack),
		core_transfer_cost_(core_transfer_cost),
		tor_transfer_cost_(tor_transfer_cost),
		percent_block_tolerance_(percent_block_tolerance),
		machines_per_rack_(machines_per_rack),
		filesystem_(dfs),
		runtime_distribution_(runtime_distribution),
		block_distribution_(block_distribution)
		{
  // Shut up unused warnings for now
  CHECK_NOTNULL(leaf_res_ids);
  CHECK_NOTNULL(task_bindings);

  // initialise to a single, empty rack
  rack_to_machine_map_.assign(1, std::list<ResourceID_t>());
}

SimulatedQuincyCostModel::~SimulatedQuincyCostModel() {
  for (auto elt : preferred_machine_map_) {
    ResourceCostMap_t *map = elt.second;
    delete map;
  }
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t SimulatedQuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
	// TODO(adam): time dependent cost;
  return 10000;
}

// The cost from the unscheduled to the sink is 0. Setting it to a value greater
// than zero affects all the unscheduled tasks. It is better to affect the cost
// of not running a task through the cost from the task to the unscheduled
// aggregator.
Cost_t SimulatedQuincyCostModel::UnscheduledAggToSinkCost(JobID_t job_id) {
  return 0ULL;
}

// The cost from the task to the cluster aggregator models how expensive is a
// task to run on any node in the cluster. The cost of the topology's arcs are
// the same for all the tasks.
Cost_t SimulatedQuincyCostModel::TaskToClusterAggCost(TaskID_t task_id) {
	return cluster_aggregator_cost_[task_id];
}

Cost_t SimulatedQuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
  ResourceCostMap_t *cost_map = preferred_machine_map_[task_id];
  return (*cost_map)[resource_id];
}

// Cost from machines to cores, always 0.
Cost_t SimulatedQuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
  return 0ULL;
}

// The cost from the resource leaf to the sink is 0.
Cost_t SimulatedQuincyCostModel::LeafResourceNodeToSinkCost(ResourceID_t resource_id) {
  return 0ULL;
}

Cost_t SimulatedQuincyCostModel::TaskContinuationCost(TaskID_t task_id) {
	// TODO(adam): task preemption support
  return 0ULL;
}

Cost_t SimulatedQuincyCostModel::TaskPreemptionCost(TaskID_t task_id) {
	// TODO(adam): task preemption support
  return 0ULL;
}

Cost_t SimulatedQuincyCostModel::TaskToEquivClassAggregator(TaskID_t task_id,
                                                   EquivClass_t tec) {
  // task to rack aggregators
  return preferred_rack_map_[task_id][tec];
}

Cost_t SimulatedQuincyCostModel::EquivClassToResourceNode(EquivClass_t tec,
                                                 ResourceID_t res_id) {
	// cost of arcs from rack aggregators are always zero
	// (costs are instead encoded in arc from task to aggregator)
	return 0LL;
}

Cost_t SimulatedQuincyCostModel::EquivClassToEquivClass(EquivClass_t tec1,
                                                        EquivClass_t tec2) {
	// this shouldn't be called; the only arcs are from cluster aggregator to rack
	// aggregator, but Firmament considers cluster aggregator to be a special case
	CHECK(false);
  return 0LL;
}

// In Quincy, a task is in its own equivalence class
vector<EquivClass_t>* SimulatedQuincyCostModel::GetTaskEquivClasses(
    TaskID_t task_id) {
  vector<EquivClass_t>* preferred_res = new vector<EquivClass_t>();
  auto &preferred_racks = preferred_rack_map_[task_id];
  for (auto elt : preferred_racks) {
    EquivClass_t rack = elt.second;
    VLOG(1) << "Task " << task_id << " preferred rack " << rack;
    preferred_res->push_back(rack);
  }
  return preferred_res;
}

vector<EquivClass_t>* SimulatedQuincyCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
	vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
	EquivClass_t rack_aggregator = machine_to_rack_map_[res_id];
	equiv_classes->push_back(rack_aggregator);
	VLOG(1) << "Machine " << task_id << " belongs to rack " << rack;
	return equiv_classes;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<ResourceID_t>* res = new vector<ResourceID_t>();
  return res;
}

vector<TaskID_t>* SimulatedQuincyCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
  vector<TaskID_t>* res = new vector<TaskID_t>();
  return res;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
  vector<ResourceID_t>* preferred_res = new vector<ResourceID_t>();
  auto &preferred_machines = *(preferred_machine_map_[task_id]);
  for (auto elt : preferred_machines) {
  	ResourceID_t machine = elt.first;
  	preferred_res->push_back(machine);
  }
  return preferred_res;
}

pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
    SimulatedQuincyCostModel::GetEquivClassToEquivClassesArcs(EquivClass_t tec) {
	vector<EquivClass_t>* incoming_arcs = new vector<EquivClass_t>();
	vector<EquivClass_t>* outgoing_arcs = new vector<EquivClass_t>();
	return pair<vector<EquivClass_t>*, vector<EquivClass_t>*>
		 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 (incoming_arcs, outgoing_arcs);
}

void SimulatedQuincyCostModel::AddMachine(
    ResourceTopologyNodeDescriptor* rtnd_ptr) {
	// we use ResourceID_t to identify machines
	ResourceID_t res_id = ResourceIDFromString(rtnd_ptr->resource_desc().uuid());
	// 'replicate' blocks
	filesystem_->addMachine(res_id);
	// bin it into a rack
	EquivClass_t current_rack = rack_to_machine_map_.size() - 1;
	if (rack_to_machine_map_[current_rack].size() >= machines_per_rack_) {
		// currrent rack is full
		current_rack++;
		rack_to_machine_map_.resize(current_rack + 1);
		rack_to_machine_map_[current_rack] = std::list<ResourceID_t>();
	}
	rack_to_machine_map_[current_rack].push_back(res_id);
	machine_to_rack_map_[res_id] = current_rack;
	VLOG(1) << "Added machine " << res_id << " to rack " << current_rack;
}

void SimulatedQuincyCostModel::RemoveMachine(ResourceID_t res_id) {
	filesystem_->removeMachine(res_id);
	EquivClass_t rack = machine_to_rack_map_[res_id];
	rack_to_machine_map_[rack].remove(res_id);
	machine_to_rack_map_.erase(res_id);
}

void SimulatedQuincyCostModel::BuildTaskFileSet(TaskID_t task_id) {
	file_map_[task_id] = std::unordered_set<SimulatedDFS::FileID_t>();
	std::unordered_set<SimulatedDFS::FileID_t> &file_set = file_map_[task_id];

	// Get runtime
	// SOMEDAY(adam): This is a giant hack. Knowledge base stores runtime by
	// task equivalence classes. Simulator assumes one equivalence class per task.
	// We DON't do this. But let's pretend we do here.
	EquivClass_t bogus_equivalence_class = (EquivClass_t)task_id;
	double avg_runtime =
	                knowledge_base_->GetAvgRuntimeForTEC(bogus_equivalence_class);
	VLOG(1) << "Task " << task_id << " has runtime " << avg_runtime;

	// Estimate how many blocks input the task has
	double cumulative_probability = runtime_distribution_->distribution(avg_runtime);
	VLOG(2) << "Which has probability " << cumulative_probability;
	uint64_t num_blocks = block_distribution_->inverse(cumulative_probability);
	VLOG(2) << "Giving " << num_blocks << " blocks";

	// Finally, select some files. Sample to get approximately the right number of blocks.
	file_set = filesystem_->sampleFiles(num_blocks, percent_block_tolerance_);
}

void SimulatedQuincyCostModel::ComputeCostsAndPreferredSet(TaskID_t task_id) {
  ResourceFrequencyMap_t machine_frequency;
  unordered_map<EquivClass_t, uint64_t> rack_frequency;
  SimulatedDFS::BlockID_t total_num_blocks = 0;

  std::unordered_set<SimulatedDFS::FileID_t> &file_set = file_map_[task_id];
  for (SimulatedDFS::FileID_t file_id : file_set) {
  	const std::pair<SimulatedDFS::BlockID_t, SimulatedDFS::BlockID_t>
  	                                  &blocks = filesystem_->getBlocks(file_id);
  	SimulatedDFS::BlockID_t start = blocks.first, end = blocks.second;
  	total_num_blocks += end - start + 1;
  	for (auto block = start; block <= end; block++) {
  		const std::list<ResourceID_t> &machines = filesystem_->getMachines(file_id);
  		std::unordered_set<EquivClass_t> racks;
  		for (ResourceID_t machine : machines) {
  			uint32_t frequency = FindWithDefault(machine_frequency, machine, 0);
  			machine_frequency[machine] = frequency + 1;

  			EquivClass_t rack = machine_to_rack_map_[machine];
  			racks.insert(rack);
  		}
  		for (EquivClass_t rack : racks) {
  			// N.B. Need to have a set and iterate over it separately to handle the
  			// case where block is stored in two machines in same rack
  			uint64_t frequency = FindWithDefault(rack_frequency, rack, 0);
  			rack_frequency[rack] = frequency + 1;
  		}
  	}
  }

  preferred_machine_map_[task_id] = new ResourceCostMap_t();
  auto &preferred_machines = *(preferred_machine_map_[task_id]);
  for (auto freq : machine_frequency) {
  	SimulatedDFS::BlockID_t num_local_blocks = freq.second;
  	double proportion = num_local_blocks / total_num_blocks;
  	if (proportion > proportion_machine_preferred_) {
  		// add to preferred list and compute cost
  		ResourceID_t machine = freq.first;
  		EquivClass_t rack = machine_to_rack_map_[machine];
  		SimulatedDFS::BlockID_t num_rack_blocks = rack_frequency[rack];

  		// local blocks are charged at 0,
  		// blocks transferred between nodes in rack charged at tor_transfer_cost_,
  		// blocks between racks charged at rack_transfer_cost_
  		// Totals so far are inclusive, calculate exclusive ones
  		num_rack_blocks -= num_local_blocks;
  		SimulatedDFS::BlockID_t num_core_blocks =
			                    total_num_blocks - num_rack_blocks - num_local_blocks;

  		Cost_t cost = num_core_blocks * core_transfer_cost_;
  		cost += num_rack_blocks * tor_transfer_cost_;

  		preferred_machines[machine] = cost;

  		// TODO
  		VLOG(2) << "Task " << task_id << " preferred machine " << machine;
  	}
  }

  preferred_rack_map_[task_id] = unordered_map<EquivClass_t, Cost_t>();
  auto &preferred_racks = preferred_rack_map_[task_id];
  for (auto freq : rack_frequency) {
  	SimulatedDFS::BlockID_t num_rack_blocks = freq.second;
  	double proportion = num_rack_blocks / total_num_blocks;
  	if (proportion > proportion_rack_preferred_) {
  		EquivClass_t rack = freq.first;

  		SimulatedDFS::BlockID_t num_core_blocks = total_num_blocks
  				 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	  - num_rack_blocks;

  		Cost_t cost = num_core_blocks * core_transfer_cost_;
  		cost += num_rack_blocks * tor_transfer_cost_;

  		preferred_racks[rack] = cost;
  	}
  }

  Cost_t cost = total_num_blocks * core_transfer_cost_;
  cluster_aggregator_cost_[task_id] = cost;
}

void SimulatedQuincyCostModel::AddTask(TaskID_t task_id) {
	BuildTaskFileSet(task_id);
	ComputeCostsAndPreferredSet(task_id);
}

void SimulatedQuincyCostModel::RemoveTask(TaskID_t task_id) {
	file_map_.erase(task_id);
}

FlowGraphNode* SimulatedQuincyCostModel::GatherStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

FlowGraphNode* SimulatedQuincyCostModel::UpdateStats(FlowGraphNode* accumulator,
                                            FlowGraphNode* other) {
  return NULL;
}

}  // namespace firmament
