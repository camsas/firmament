// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/dfs/simulated_data_layer_manager.h"

#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"
#include "sim/google_runtime_distribution.h"

// Distributed filesystem options
DEFINE_uint64(simulated_quincy_blocks_per_machine, 98304,
              "Number of 64 MB blocks each machine stores. "
              "Defaults to 98304, i.e. 6 TB.");
DEFINE_uint64(simulated_quincy_replication_factor, 3,
              "The number of times each block should be replicated.");
// File size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_file_percent_min, 20,
              "Percentage of files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_file_min_blocks, 1,
              "Minimum # of blocks in file.");
DEFINE_double(simulated_quincy_file_max_blocks, 160,
              "Maximum # of blocks in file.");
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

// Random seed
DEFINE_uint64(simulated_quincy_random_seed, 42, "Seed for random generators.");

namespace firmament {
namespace sim {

SimulatedDataLayerManager::SimulatedDataLayerManager() {
  input_block_dist_ =
    new GoogleBlockDistribution(FLAGS_simulated_quincy_input_percent_min,
                                FLAGS_simulated_quincy_input_min_blocks,
                                FLAGS_simulated_quincy_input_max_blocks);
  runtime_dist_ =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);
  file_block_dist_ =
    new GoogleBlockDistribution(FLAGS_simulated_quincy_file_percent_min,
                                FLAGS_simulated_quincy_file_min_blocks,
                                FLAGS_simulated_quincy_file_max_blocks);
  dfs_ = new SimulatedDFS(file_block_dist_,
                          FLAGS_simulated_quincy_blocks_per_machine,
                          FLAGS_simulated_quincy_replication_factor,
                          FLAGS_simulated_quincy_random_seed);
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
  delete input_block_dist_;
  delete runtime_dist_;
  delete file_block_dist_;
  delete dfs_;
}

list<DataLocation> SimulatedDataLayerManager::GetFileLocations() {
  // TODO(ionel): Implement!
  list<DataLocation> test;
  return test;
}

} // namespace sim
} // namespace firmament
