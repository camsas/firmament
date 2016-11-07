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

#include "sim/dfs/simulated_bounded_dfs.h"

#include <SpookyV2.h>

#include "base/common.h"
#include "base/units.h"

#define MACHINE_POOL_SEED 42
#define MAX_SAMPLE_POOL 3

DECLARE_uint64(simulated_dfs_replication_factor);
DECLARE_uint64(simulated_block_size);

namespace firmament {
namespace sim {

SimulatedBoundedDFS::SimulatedBoundedDFS(TraceGenerator* trace_generator)
  : SimulatedUniformDFS(trace_generator) {
}

SimulatedBoundedDFS::~SimulatedBoundedDFS() {
  // trace_generator_ is not owned by SimulatedBoundedDFS.
}

void SimulatedBoundedDFS::AddBlocksForTask(const TaskDescriptor& td,
                                           uint64_t num_blocks,
                                           uint64_t max_machine_spread) {
  vector<ResourceID_t> machines;
  max_machine_spread *= FLAGS_simulated_dfs_replication_factor;
  // Make sure max_machine_spread is not larger than the number of machines with
  // free space the cluster has.
  max_machine_spread = min(max_machine_spread, machine_num_free_blocks_.size());
  // NOTE: This is inefficient because we compute the pool for every task.
  GetJobMachinePool(td.job_id(), max_machine_spread, &machines);
  TaskID_t task_id = td.uid();
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    for (uint64_t replica_index = 0;
         replica_index < FLAGS_simulated_dfs_replication_factor;
         replica_index++) {
      ResourceID_t machine_res_id =
        PlaceBlockOnMachinesPool(task_id, block_id, &machines);
      unordered_set<TaskID_t>* tasks_machine =
        FindOrNull(tasks_on_machine_, machine_res_id);
      CHECK_NOTNULL(tasks_machine);
      tasks_machine->insert(task_id);
      DataLocation data_location(machine_res_id,
                                 GetRackForMachine(machine_res_id),
                                 block_id,
                                 FLAGS_simulated_block_size);
      task_to_data_locations_.insert(
          pair<TaskID_t, DataLocation>(task_id, data_location));
      trace_generator_->AddBlock(machine_res_id, block_id,
                                 data_location.size_bytes_);
    }
  }
}

ResourceID_t SimulatedBoundedDFS::PlaceBlockOnMachinesPool(
    TaskID_t task_id,
    uint64_t block_id,
    vector<ResourceID_t>* machines) {
  uint64_t num_machines_sampled = 0;
  ResourceID_t machine_res_id;
  uint64_t* num_free_blocks;
  do {
    uint32_t machine_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % machines->size();
    num_free_blocks =
      FindOrNull(machine_num_free_blocks_, (*machines)[machine_index]);
    CHECK_NOTNULL(num_free_blocks);
    ++num_machines_sampled;
    if (num_machines_sampled > machines->size()) {
      // It's time to refresh the pool because we've likely run out space.
      uint64_t num_machines = machines->size();
      machines->clear();
      GetRandomMachinePool(num_machines, machines);
      num_machines_sampled = 0;
    }
    machine_res_id = (*machines)[machine_index];
  } while (*num_free_blocks == 0);
  *num_free_blocks = *num_free_blocks - 1;
  return machine_res_id;
}

void SimulatedBoundedDFS::GetRandomMachinePool(
    uint64_t num_machines,
    vector<ResourceID_t>* machines) {
  uint64_t num_machines_sampled = 0;
  while (machines->size() < num_machines) {
    if (num_machines_sampled > MAX_SAMPLE_POOL * num_machines) {
      LOG(FATAL) << "Could not find " << num_machines
                 << " with available space";
    }
    uint32_t machine_index =
      static_cast<uint32_t>(rand_r(&rand_seed_)) % machines_.size();
    ResourceID_t machine_res_id = machines_[machine_index];
    uint64_t* num_free_blocks =
      FindOrNull(machine_num_free_blocks_, machine_res_id);
    CHECK_NOTNULL(num_free_blocks);
    ++num_machines_sampled;
    if (*num_free_blocks > 0) {
      machines->push_back(machine_res_id);
    }
  }
}

void SimulatedBoundedDFS::GetJobMachinePool(const string& job_id,
                                            uint64_t num_tasks,
                                            vector<ResourceID_t>* machines) {
  uint32_t machine_rand_seed =
    SpookyHash::Hash32(job_id.c_str(), sizeof(char) * job_id.length(),
                       MACHINE_POOL_SEED);
  while (machines->size() < num_tasks) {
    uint32_t machine_index =
      static_cast<uint32_t>(rand_r(&machine_rand_seed)) % machines_.size();
    machines->push_back(machines_[machine_index]);
  }
}

} // namespace sim
} // namespace firmament
