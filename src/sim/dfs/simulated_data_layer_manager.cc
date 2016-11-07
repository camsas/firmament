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

#include "sim/dfs/simulated_data_layer_manager.h"

#include "base/units.h"
#include "misc/map-util.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_bounded_dfs.h"
#include "sim/dfs/simulated_hdfs.h"
#include "sim/dfs/simulated_skewed_dfs.h"
#include "sim/dfs/simulated_uniform_dfs.h"
#include "sim/google_runtime_distribution.h"

// See google_runtime_distribution.h for explanation of these defaults
DEFINE_double(simulated_quincy_runtime_factor, 0.298,
              "Runtime power law distribution: factor parameter.");
DEFINE_double(simulated_quincy_runtime_power, -0.2627,
              "Runtime power law distribution: power parameter.");
// Distributed filesystem options
DEFINE_uint64(simulated_block_size, 536870912,
              "The size of a DFS block in bytes");
DEFINE_uint64(simulated_dfs_blocks_per_machine, 12288,
              "Number of blocks each machine stores. "
              "Defaults to 12288, i.e. 6 TB for 512MB blocks.");
DEFINE_uint64(simulated_dfs_replication_factor, 3,
              "The number of times each block should be replicated.");
DEFINE_string(simulated_dfs_type, "bounded", "The type of DFS to simulated. "
              "Options: uniform | bounded | hdfs | skewed");


namespace firmament {
namespace sim {

SimulatedDataLayerManager::SimulatedDataLayerManager(
    TraceGenerator* trace_generator) {
  input_block_dist_ = new GoogleBlockDistribution();
  runtime_dist_ =
    new GoogleRuntimeDistribution(FLAGS_simulated_quincy_runtime_factor,
                                  FLAGS_simulated_quincy_runtime_power);
  if (!FLAGS_simulated_dfs_type.compare("uniform")) {
    dfs_ = new SimulatedUniformDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("bounded")) {
    dfs_ = new SimulatedBoundedDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("hdfs")) {
    dfs_ = new SimulatedHDFS(trace_generator);
  } else if (!FLAGS_simulated_dfs_type.compare("skewed")) {
    dfs_ = new SimulatedSkewedDFS(trace_generator);
  } else {
    LOG(FATAL) << "Unexpected simulated DFS type: " << FLAGS_simulated_dfs_type;
  }
}

SimulatedDataLayerManager::~SimulatedDataLayerManager() {
  delete input_block_dist_;
  delete runtime_dist_;
  delete dfs_;
}

EquivClass_t SimulatedDataLayerManager::AddMachine(
    const string& hostname,
    ResourceID_t machine_res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, machine_res_id));
  return dfs_->AddMachine(machine_res_id);
}

void SimulatedDataLayerManager::GetFileLocations(
    const string& file_path, list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  dfs_->GetFileLocations(file_path, locations);
}

int64_t SimulatedDataLayerManager::GetFileSize(const string& file_path) {
  // TODO(ionel): Implement!
  return 0;
}

bool SimulatedDataLayerManager::RemoveMachine(const string& hostname) {
  ResourceID_t* machine_res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(machine_res_id);
  ResourceID_t res_id_tmp = *machine_res_id;
  hostname_to_res_id_.erase(hostname);
  return dfs_->RemoveMachine(res_id_tmp);
}

uint64_t SimulatedDataLayerManager::AddFilesForTask(
    const TaskDescriptor& td,
    uint64_t avg_runtime,
    bool long_running_service,
    uint64_t max_machine_spread) {
  if (!long_running_service) {
    double cumulative_probability =
      runtime_dist_->ProportionShorterTasks(avg_runtime);
    uint64_t input_size = input_block_dist_->Inverse(cumulative_probability);
    uint64_t num_blocks = input_size / FLAGS_simulated_block_size;
    // Need to increase if there was a remainder, since integer division
    // truncates.
    if (input_size % FLAGS_simulated_block_size != 0) {
      num_blocks++;
    }
    dfs_->AddBlocksForTask(td, num_blocks, max_machine_spread);
    return num_blocks * FLAGS_simulated_block_size;
  } else {
    return 0;
  }
}

void SimulatedDataLayerManager::RemoveFilesForTask(const TaskDescriptor& td) {
  dfs_->RemoveBlocksForTask(td.uid());
}

} // namespace sim
} // namespace firmament
