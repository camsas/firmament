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

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_HDFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_HDFS_H

#include "sim/dfs/simulated_uniform_dfs.h"

namespace firmament {
namespace sim {

class SimulatedHDFS : public SimulatedUniformDFS {
 public:
  SimulatedHDFS(TraceGenerator* trace_generator);
  ~SimulatedHDFS();

  void AddBlocksForTask(const TaskDescriptor& td, uint64_t num_blocks,
                        uint64_t max_machine_spread);
 private:
  EquivClass_t PickDifferentRack(EquivClass_t rack_id);
  void PlaceBlocksInRack(EquivClass_t rack_id, TaskID_t task_id,
                         uint64_t block_id);

};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_HDFS_H
