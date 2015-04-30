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
		double delta_preferred_machine,
		double delta_preferred_rack,
		Cost_t core_transfer_cost,
		Cost_t tor_transfer_cost,
		uint32_t percent_block_tolerance,
		uint64_t machines_per_rack,
		SimulatedDFS* dfs)
  : resource_map_(resource_map),
    job_map_(job_map),
    task_map_(task_map),
    task_bindings_(task_bindings),
    leaf_res_ids_(leaf_res_ids),
    knowledge_base_(kb),
		proportion_machine_preferred_(delta_preferred_machine),
		proportion_rack_preferred_(delta_preferred_rack),
		core_transfer_cost_(core_transfer_cost),
		tor_transfer_cost_(tor_transfer_cost),
		percent_block_tolerance_(percent_block_tolerance),
		machines_per_rack_(machines_per_rack),
		filesystem_(dfs),
		// see google_runtime_distribution.h for explanation of these numbers
		runtime_distribution_(0.298, -0.2627),
		// these are scaled up number of blocks to get a collection of files
		// XXX(adam): come up with realistic numbers
		block_distribution_(10, 1, 167772160)
		{
	// TODO
  //application_stats_ = knowledge_base_->AppStats();
  CHECK_NOTNULL(task_bindings_);
  // initialise to a single, empty rack
  rack_to_machine_map_.assign(1, std::list<ResourceID_t>());
}

// The cost of leaving a task unscheduled should be higher than the cost of
// scheduling it.
Cost_t SimulatedQuincyCostModel::TaskToUnscheduledAggCost(TaskID_t task_id) {
	// TODO
  int64_t half_max_arc_cost = FLAGS_flow_max_arc_cost / 2;
  return half_max_arc_cost + rand_r(&rand_seed_) % half_max_arc_cost + 1;
  //  return 5ULL;
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
	// TODO
  vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
  CHECK_GT(equiv_classes->size(), 0);
  // Avg runtime is in milliseconds, so we convert it to tenths of a second
  uint64_t avg_runtime =
    knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());
  delete equiv_classes;
  return (avg_runtime * 100);
}

Cost_t SimulatedQuincyCostModel::TaskToResourceNodeCost(TaskID_t task_id,
                                               ResourceID_t resource_id) {
	// TODO
  return rand() % (FLAGS_flow_max_arc_cost / 3) + 1;
}

Cost_t SimulatedQuincyCostModel::ResourceNodeToResourceNodeCost(
    ResourceID_t source,
    ResourceID_t destination) {
	// TODO
	// XXX: When is this called?
  return rand() % (FLAGS_flow_max_arc_cost / 4) + 1;
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
	// TODO
  return rand() % (FLAGS_flow_max_arc_cost / 2) + 1;
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
  vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
  TaskDescriptor* td_ptr = FindPtrOrNull(*task_map_, task_id);
  CHECK_NOTNULL(td_ptr);
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, td_ptr->binary());
  equiv_classes->push_back(static_cast<EquivClass_t>(hash));
  return equiv_classes;
}

vector<EquivClass_t>* SimulatedQuincyCostModel::GetResourceEquivClasses(
    ResourceID_t res_id) {
	vector<EquivClass_t>* equiv_classes = new vector<EquivClass_t>();
	EquivClass_t rack_aggregator = machine_to_rack_map_[res_id];
	equiv_classes->push_back(rack_aggregator);
	return equiv_classes;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetOutgoingEquivClassPrefArcs(
    EquivClass_t tec) {
	// TODO
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  CHECK_GE(leaf_res_ids_->size(), FLAGS_num_pref_arcs_task_to_res);
  uint32_t rand_seed_ = 0;
  for (uint32_t num_arc = 0; num_arc < FLAGS_num_pref_arcs_task_to_res;
       ++num_arc) {
    size_t index = rand_r(&rand_seed_) % leaf_res_ids_->size();
    unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid>>::iterator it =
      leaf_res_ids_->begin();
    advance(it, index);
    prefered_res->push_back(*it);
  }
  return prefered_res;
}

vector<TaskID_t>* SimulatedQuincyCostModel::GetIncomingEquivClassPrefArcs(
    EquivClass_t tec) {
	// TODO: rack aggregators?
  LOG(FATAL) << "Not implemented!";
  return NULL;
}

vector<ResourceID_t>* SimulatedQuincyCostModel::GetTaskPreferenceArcs(TaskID_t task_id) {
	// TODO: have some preferences
  vector<ResourceID_t>* prefered_res = new vector<ResourceID_t>();
  return prefered_res;
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
	vector<EquivClass_t>* equiv_classes = GetTaskEquivClasses(task_id);
	CHECK_GT(equiv_classes->size(), 0);
	uint64_t avg_runtime = knowledge_base_->GetAvgRuntimeForTEC(equiv_classes->front());

	// Estimate how many blocks input the task has
	double cumulative_probability = runtime_distribution_.distribution(avg_runtime);
	uint64_t num_blocks = block_distribution_.inverse(cumulative_probability);

	// Finally, select some files. Sample to get approximately the right number of blocks.
	file_set = filesystem_->sampleFiles(num_blocks, percent_block_tolerance_);
}

void SimulatedQuincyCostModel::ComputeCostsAndPreferredSet(TaskID_t task_id) {
  unordered_map<ResourceID_t, uint64_t> machine_frequency;
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

  preferred_machine_map_[task_id] = std::list<std::pair<ResourceID_t, Cost_t>>();
  auto &preferred_machine_list = preferred_machine_map_[task_id];
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

  		preferred_machine_list.push_back(make_pair(machine, cost));
  	}
  }

  preferred_rack_map_[task_id] = std::list<std::pair<EquivClass_t, Cost_t>>();
  auto &preferred_rack_list = preferred_rack_map_[task_id];
  for (auto freq : rack_frequency) {
  	SimulatedDFS::BlockID_t num_rack_blocks = freq.second;
  	double proportion = num_rack_blocks / total_num_blocks;
  	if (proportion > proportion_rack_preferred_) {
  		EquivClass_t rack = freq.first;

  		SimulatedDFS::BlockID_t num_core_blocks = total_num_blocks
  				 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	 	  - num_rack_blocks;

  		Cost_t cost = num_core_blocks * core_transfer_cost_;
  		cost += num_rack_blocks * tor_transfer_cost_;

  		preferred_rack_list.push_back(make_pair(rack, cost));
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
