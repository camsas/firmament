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

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_BOUNDED_DFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_BOUNDED_DFS_H

#include "sim/dfs/simulated_uniform_dfs.h"

namespace firmament {
namespace sim {

class SimulatedBoundedDFS : public SimulatedUniformDFS {
 public:
  SimulatedBoundedDFS(TraceGenerator* trace_generator);
  ~SimulatedBoundedDFS();

  void AddBlocksForTask(const TaskDescriptor& td, uint64_t num_blocks,
                        uint64_t max_machine_spread);
 private:
  ResourceID_t PlaceBlockOnMachinesPool(TaskID_t task_id, uint64_t block_id,
                                        vector<ResourceID_t>* machines);
  void GetJobMachinePool(const string& job_id, uint64_t num_tasks,
                         vector<ResourceID_t>* machines);
  void GetRandomMachinePool(uint64_t num_machines,
                            vector<ResourceID_t>* machines);
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_BOUNDED_DFS_H
