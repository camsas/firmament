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

#include "sim/dfs/simulated_hdfs.h"

#include "base/common.h"
#include "base/units.h"

#define SEED 42

DECLARE_uint64(simulated_dfs_replication_factor);
DECLARE_uint64(simulated_block_size);

namespace firmament {
namespace sim {

SimulatedHDFS::SimulatedHDFS(TraceGenerator* trace_generator)
  : SimulatedUniformDFS(trace_generator) {
}

SimulatedHDFS::~SimulatedHDFS() {
  // trace_generator_ is not owned by SimulatedBoundedDFS.
}

void SimulatedHDFS::AddBlocksForTask(const TaskDescriptor& td,
                                     uint64_t num_blocks,
                                     uint64_t max_machine_spread) {
  TaskID_t task_id = td.uid();
  ResourceID_t local_machine_id;
  uint64_t* num_free_blocks;
  uint64_t num_machines_sampled = 0;
  do {
    uint32_t machine_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % machines_.size();
    local_machine_id = machines_[machine_index];
    num_free_blocks = FindOrNull(machine_num_free_blocks_, local_machine_id);
    CHECK_NOTNULL(num_free_blocks);
    ++num_machines_sampled;
    if (num_machines_sampled > machines_.size()) {
      LOG(FATAL) << "Not enough space in the cluster";
    }
  } while (*num_free_blocks < num_blocks);
  *num_free_blocks = *num_free_blocks - num_blocks;
  EquivClass_t rack_id = GetRackForMachine(local_machine_id);
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    unordered_set<TaskID_t>* tasks_machine =
      FindOrNull(tasks_on_machine_, local_machine_id);
    CHECK_NOTNULL(tasks_machine);
    tasks_machine->insert(task_id);
    DataLocation data_location(local_machine_id, rack_id, block_id,
                               FLAGS_simulated_block_size);
    task_to_data_locations_.insert(
        pair<TaskID_t, DataLocation>(task_id, data_location));
    trace_generator_->AddBlock(local_machine_id, block_id,
                               data_location.size_bytes_);
    // Place the other replicas in a different rack.
    EquivClass_t other_rack_id = PickDifferentRack(rack_id);
    PlaceBlocksInRack(other_rack_id, task_id, block_id);
  }
}

EquivClass_t SimulatedHDFS::PickDifferentRack(EquivClass_t rack_id) {
  vector<EquivClass_t> rack_ids;
  GetRackIDs(&rack_ids);
  CHECK_GE(rack_ids.size(), 2);
  EquivClass_t new_rack_id;
  do {
    uint32_t rack_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % rack_ids.size();
    new_rack_id = rack_ids[rack_index];
  } while (new_rack_id == rack_id);
  return new_rack_id;
}

void SimulatedHDFS::PlaceBlocksInRack(EquivClass_t rack_id, TaskID_t task_id,
                                      uint64_t block_id) {
  const auto& machines_in_rack_set = GetMachinesInRack(rack_id);
  CHECK_GE(machines_in_rack_set.size(), 1);
  vector<ResourceID_t> machines_in_rack(machines_in_rack_set.begin(),
                                        machines_in_rack_set.end());
  for (uint64_t replica_index = 1;
       replica_index < FLAGS_simulated_dfs_replication_factor;
       replica_index++) {
    ResourceID_t machine_res_id;
    uint64_t* num_free_blocks;
    uint64_t num_machines_sampled = 0;
    do {
      uint32_t machine_index =
        static_cast<uint32_t>(rand_r(&rand_seed_)) % machines_in_rack.size();
      machine_res_id = machines_in_rack[machine_index];
      num_free_blocks = FindOrNull(machine_num_free_blocks_, machine_res_id);
      CHECK_NOTNULL(num_free_blocks);
      ++num_machines_sampled;
      if (num_machines_sampled > machines_in_rack.size()) {
        LOG(FATAL) << "Not enough space in the rack";
      }
    } while (*num_free_blocks == 0);
    *num_free_blocks = *num_free_blocks - 1;
    unordered_set<TaskID_t>* tasks_machine =
      FindOrNull(tasks_on_machine_, machine_res_id);
    CHECK_NOTNULL(tasks_machine);
    tasks_machine->insert(task_id);
    DataLocation machine_data_location(machine_res_id, rack_id,
                                       block_id, FLAGS_simulated_block_size);
    task_to_data_locations_.insert(
        pair<TaskID_t, DataLocation>(task_id, machine_data_location));
    trace_generator_->AddBlock(machine_res_id, block_id,
                               machine_data_location.size_bytes_);
  }
}

} // namespace sim
} // namespace firmament
