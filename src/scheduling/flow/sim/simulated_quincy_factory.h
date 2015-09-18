// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// Simulated quincy cost model setting values.

#ifndef FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_FACTORY_H
#define FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_FACTORY_H

#include "scheduling/flow/sim/google_runtime_distribution.h"
#include "scheduling/knowledge_base.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"

// Racks contain "between 29 and 31 computers" in Quincy test setup
DEFINE_uint64(simulated_quincy_machines_per_rack, 30,
              "Machines are binned into racks of specified size.");
// Defaults from Quincy paper
DEFINE_double(simulated_quincy_delta_preferred_machine, 0.1,
              "Threshold of proportion of data stored on machine for it to be "
              "on preferred list.");
DEFINE_double(simulated_quincy_delta_preferred_rack, 0.1,
              "Threshold of proportion of data stored on rack for it to be on "
              "preferred list.");
DEFINE_uint64(simulated_quincy_tor_transfer_cost, 1,
              "Cost per unit of data transferred in core switch.");
// Cost was 2 for most experiments, 20 for constrained network experiments
DEFINE_uint64(simulated_quincy_core_transfer_cost, 2,
              "Cost per unit of data transferred in core switch.");
// Distributed filesystem options
DEFINE_uint64(simulated_quincy_blocks_per_machine, 98304,
              "Number of 64 MB blocks each machine stores. "
              "Defaults to 98304, i.e. 6 TB.");
DEFINE_uint64(simulated_quincy_replication_factor, 3,
              "The number of times each block should be replicated.");
// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
              "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
              "Runtime power law distribution: factor parameter.");
// Input size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_input_percent_min, 50,
              "Percentage of input files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_input_min_blocks, 1,
              "Minimum # of blocks in input file.");
DEFINE_double(simulated_quincy_input_max_blocks, 320,
              "Maximum # of blocks in input file.");
DEFINE_uint64(simulated_quincy_input_percent_over_tolerance, 50,
              "Percentage # of blocks allowed to exceed the value predicted.");
// File size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_file_percent_min, 20,
              "Percentage of files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_file_min_blocks, 1,
              "Minimum # of blocks in file.");
DEFINE_double(simulated_quincy_file_max_blocks, 160,
              "Maximum # of blocks in file.");
// Random seed
DEFINE_uint64(simulated_quincy_random_seed, 42, "Seed for random generators.");

using firmament::sim::dfs::GoogleBlockDistribution;
using firmament::sim::dfs::SimulatedDFS;

namespace firmament {
namespace scheduler {

SimulatedQuincyCostModel* SetupSimulatedQuincyCostModel(
    shared_ptr<ResourceMap_t> resource_map,
    shared_ptr<JobMap_t> job_map,
    shared_ptr<TaskMap_t> task_map,
    shared_ptr<KnowledgeBase> knowledge_base,
    unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>* leaf_res_ids) {
  GoogleBlockDistribution* input_block_distn =
    new GoogleBlockDistribution(FLAGS_simulated_quincy_input_percent_min,
                                FLAGS_simulated_quincy_input_min_blocks,
                                FLAGS_simulated_quincy_input_max_blocks);

  GoogleRuntimeDistribution* runtime_distn =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);

  GoogleBlockDistribution* file_block_distn =
    new GoogleBlockDistribution(FLAGS_simulated_quincy_file_percent_min,
                                FLAGS_simulated_quincy_file_min_blocks,
                                FLAGS_simulated_quincy_file_max_blocks);
  SimulatedDFS* dfs =
    new SimulatedDFS(file_block_distn,
                     FLAGS_simulated_quincy_blocks_per_machine,
                     FLAGS_simulated_quincy_replication_factor,
                     FLAGS_simulated_quincy_random_seed);


  return new SimulatedQuincyCostModel(
      resource_map, job_map, task_map, leaf_res_ids,
      knowledge_base, dfs, runtime_distn, input_block_distn,
      FLAGS_simulated_quincy_delta_preferred_machine,
      FLAGS_simulated_quincy_delta_preferred_rack,
      FLAGS_simulated_quincy_core_transfer_cost,
      FLAGS_simulated_quincy_tor_transfer_cost,
      FLAGS_simulated_quincy_input_percent_over_tolerance,
      FLAGS_simulated_quincy_machines_per_rack);
}

} // namespace scheduler
} // namespace firmament

#endif  // FIRMAMENT_SCHEDULING_FLOW_SIM_SIMULATED_QUINCY_FACTORY_H
