// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/dfs/simulated_data_layer_manager.h"

#include "misc/map-util.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"
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
  dfs_ = new SimulatedDFS();
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
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

void SimulatedDataLayerManager::AddFilesForTask(TaskID_t task_id,
                                                uint64_t avg_runtime) {
  double cumulative_probability =
    runtime_dist_->ProportionShorterTasks(avg_runtime);
  uint64_t num_blocks = input_block_dist_->Inverse(cumulative_probability);
  dfs_->AddBlocksForTask(task_id, num_blocks);
}

void SimulatedDataLayerManager::RemoveFilesForTask(TaskID_t task_id) {
  dfs_->RemoveBlocksForTask(task_id);
}

} // namespace sim
} // namespace firmament
