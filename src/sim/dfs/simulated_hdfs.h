// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
