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

#include "sim/dfs/simulated_skewed_dfs.h"

DECLARE_uint64(simulated_dfs_replication_factor);
DECLARE_uint64(simulated_block_size);

namespace firmament {
namespace sim {

SimulatedSkewedDFS::SimulatedSkewedDFS(TraceGenerator* trace_generator)
  : SimulatedUniformDFS(trace_generator), pareto_dist_(1, 1.041392685),
    rand_gen_(15), uniform_real_(0.0, 1.0),
    generator_(rand_gen_, uniform_real_) {
}

SimulatedSkewedDFS::~SimulatedSkewedDFS() {
  // trace_generator_ is not owned by SimulatedSkewedDFS.
}

void SimulatedSkewedDFS::AddBlocksForTask(const TaskDescriptor& td,
                                          uint64_t num_blocks,
                                          uint64_t max_machine_spread) {
  TaskID_t task_id = td.uid();
  for (uint64_t block_index = 0; block_index < num_blocks; ++block_index) {
    uint64_t block_id = GenerateBlockID(task_id, block_index);
    trace_generator_->AddTaskInputBlock(td, block_id);
    for (uint64_t replica_index = 0;
         replica_index < FLAGS_simulated_dfs_replication_factor;
         replica_index++) {
      ResourceID_t machine_res_id = GetMachineForNewBlock();
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

ResourceID_t SimulatedSkewedDFS::GetMachineForNewBlock() {
  uint32_t machine_pareto_index =
    static_cast<uint32_t>(round(boost::math::quantile(pareto_dist_,
                                                      generator_())));
  uint32_t max_machine_index = static_cast<uint32_t>(machines_.size() - 1);
  uint32_t machine_index = min(machine_pareto_index, max_machine_index);
  return machines_[machine_index];
}

} // namespace sim
} // namespace firmament
