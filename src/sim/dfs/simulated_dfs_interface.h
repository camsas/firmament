// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DFS_INTERFACE_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DFS_INTERFACE_H

#include "base/common.h"
#include "base/types.h"
#include "scheduling/data_layer_manager_interface.h"

namespace firmament {
namespace sim {

class SimulatedDFSInterface {
 public:
  virtual ~SimulatedDFSInterface() {};
  /**
   * Add num_blocks for a new task.
   * @param td the descriptor of the new task
   * @param num_blocks the number of blocks to add
   * @param max_machine_spread the maximum number of machines over which
   * the task's inputs should be spread.
   */
  virtual void AddBlocksForTask(const TaskDescriptor& td,
                                uint64_t num_blocks,
                                uint64_t max_machine_spread) = 0;

  /**
   * Add a new machine to the DFS.
   * @param machine_res_id the resource id of the new machine
   */
  virtual void AddMachine(ResourceID_t machine_res_id) = 0;
  virtual void GetFileLocations(const string& file_path,
                                list<DataLocation>* locations) = 0;
  /**
   * Remove all the blocks of a task.
   * @param task_id the id of the task for which to remove the blocks
   */
  virtual void RemoveBlocksForTask(TaskID_t task_id) = 0;

  /**
   * Remove a machine from the DFS. This method also removes all the blocks from
   * the machine and makes sure they're again replicated.
   * @param machine_res_id the resource id of the machine to be removed
   */
  virtual void RemoveMachine(ResourceID_t machine_res_id) = 0;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DFS_H
