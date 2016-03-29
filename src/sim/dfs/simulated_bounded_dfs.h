// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
