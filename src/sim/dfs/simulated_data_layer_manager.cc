// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/dfs/simulated_data_layer_manager.h"

#include "base/units.h"
#include "misc/map-util.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_bounded_dfs.h"
#include "sim/dfs/simulated_uniform_dfs.h"
#include "sim/google_runtime_distribution.h"

// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
              "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
              "Runtime power law distribution: power parameter.");
// Input size distribution. See Evaluation Plan for derivation of defaults.
DEFINE_uint64(simulated_quincy_input_percent_min, 50,
              "Percentage of input files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_input_min_blocks, 1,
              "Minimum # of blocks in input file.");
DEFINE_double(simulated_quincy_input_max_blocks, 320,
              "Maximum # of blocks in input file.");
DEFINE_uint64(simulated_quincy_block_size, 64, "The size of a DFS block in MB");
// Distributed filesystem options
DEFINE_uint64(simulated_dfs_blocks_per_machine, 98304,
              "Number of 64 MB blocks each machine stores. "
              "Defaults to 98304, i.e. 6 TB.");
DEFINE_uint64(simulated_dfs_replication_factor, 3,
              "The number of times each block should be replicated.");
DEFINE_string(simulated_dfs_type, "bounded", "The type of DFS to simulated. "
              "Options: uniform | bounded");

namespace firmament {
namespace sim {

SimulatedDataLayerManager::SimulatedDataLayerManager(
    TraceGenerator* trace_generator) : trace_generator_(trace_generator) {
  input_block_dist_ =
    new GoogleBlockDistribution(FLAGS_simulated_quincy_input_percent_min,
                                FLAGS_simulated_quincy_input_min_blocks,
                                FLAGS_simulated_quincy_input_max_blocks);
  runtime_dist_ =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);
  if (!FLAGS_simulated_dfs_type.compare("uniform")) {
    dfs_ = new SimulatedUniformDFS(trace_generator_);
  } else if (!FLAGS_simulated_dfs_type.compare("bounded")) {
    dfs_ = new SimulatedBoundedDFS(trace_generator_);
  } else {
    LOG(FATAL) << "Unexpected simulated DFS type: " << FLAGS_simulated_dfs_type;
  }
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
  // trace_generator_ is not owned by SimulatedDataLayerManager.
  delete input_block_dist_;
  delete runtime_dist_;
  delete dfs_;
}

void SimulatedDataLayerManager::AddMachine(const string& hostname,
                                           ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, machine_res_id));
  dfs_->AddMachine(machine_res_id);
}

void SimulatedDataLayerManager::GetFileLocations(
    const string& file_path, list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  dfs_->GetFileLocations(file_path, locations);
}

void SimulatedDataLayerManager::RemoveMachine(const string& hostname) {
  ResourceID_t* machine_res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(machine_res_id);
  dfs_->RemoveMachine(*machine_res_id);
  hostname_to_res_id_.erase(hostname);
}

uint64_t SimulatedDataLayerManager::AddFilesForTask(
    const TaskDescriptor& td,
    uint64_t avg_runtime,
    bool long_running_service,
    uint64_t max_machine_spread) {
  if (!long_running_service) {
    double cumulative_probability =
      runtime_dist_->ProportionShorterTasks(avg_runtime);
    uint64_t num_blocks = input_block_dist_->Inverse(cumulative_probability);
    dfs_->AddBlocksForTask(td, num_blocks, max_machine_spread);
    return num_blocks * FLAGS_simulated_quincy_block_size * MB_TO_BYTES;
  } else {
    return 0;
  }
}

void SimulatedDataLayerManager::RemoveFilesForTask(const TaskDescriptor& td) {
  dfs_->RemoveBlocksForTask(td.uid());
}

} // namespace sim
} // namespace firmament
