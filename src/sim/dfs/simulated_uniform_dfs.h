// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_UNIFORM_DFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_UNIFORM_DFS_H

#include "sim/dfs/simulated_dfs.h"

#include <list>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/types.h"
#include "misc/trace_generator.h"
#include "scheduling/data_layer_manager_interface.h"
#include "sim/dfs/google_block_distribution.h"

namespace firmament {
namespace sim {

class SimulatedUniformDFS : public SimulatedDFS {
 public:
  SimulatedUniformDFS(TraceGenerator* trace_generator);
  virtual ~SimulatedUniformDFS();

  /**
   * Add num_blocks for a new task.
   * @param td the descriptor of the new task
   * @param num_blocks the number of blocks to add
   * @param max_machine_spread the maximum number of machine over which
   * the task's inputs should be spread.
   */
  virtual void AddBlocksForTask(const TaskDescriptor& td, uint64_t num_blocks,
                                uint64_t max_machine_spread);

  /**
   * Add a new machine to the DFS.
   * @param machine_res_id the resource id of the new machine
   * @return the id of the rack in which the machine is located
   */
  EquivClass_t AddMachine(ResourceID_t machine_res_id);
  void GetFileLocations(const string& file_path, list<DataLocation>* locations);

  /**
   * Remove all the blocks of a task.
   * @param task_id the id of the task for which to remove the blocks
   */
  void RemoveBlocksForTask(TaskID_t task_id);

  /**
   * Remove a machine from the DFS. This method also removes all the blocks from
   * the machine and makes sure they're again
   * FLAGS_simulated_dfs_replication_factor replicated.
   * @param machine_res_id the resource id of the machine to be removed
   * @return true if the machine's rack no longer contains machines
   */
  bool RemoveMachine(ResourceID_t machine_res_id);

 protected:
  uint64_t GenerateBlockID(TaskID_t task_id, uint64_t block_index);
  void PlaceBlockOnMachines(TaskID_t task_id, uint64_t block_id);
  /**
   * Randomly places a block on a machine which has enough free space to
   * store the block.
   * @return the resource id of the machine on which the block was placed
   */
  ResourceID_t PlaceBlockOnRandomMachine();

  // Map storing the number of available blocks each machine has.
  unordered_map<ResourceID_t, uint64_t, boost::hash<boost::uuids::uuid>>
    machine_num_free_blocks_;
  vector<ResourceID_t> machines_;
  // Mapping from machines to the tasks that have blocks on the machine.
  unordered_map<ResourceID_t, unordered_set<TaskID_t>,
    boost::hash<boost::uuids::uuid>> tasks_on_machine_;
  // Mapping storing the block locations for every task.
  unordered_multimap<TaskID_t, DataLocation> task_to_data_locations_;
  uint32_t rand_seed_;
  TraceGenerator* trace_generator_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_UNIFORM_DFS_H
