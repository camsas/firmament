// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DFS_H

#include "base/common.h"
#include "base/types.h"
#include "misc/map-util.h"
#include "scheduling/data_layer_manager_interface.h"

namespace firmament {
namespace sim {

class SimulatedDFS {
 public:
  SimulatedDFS();
  virtual ~SimulatedDFS() {};
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
   * @return the id of the rack in which the machine is located
   */
  virtual EquivClass_t AddMachine(ResourceID_t machine_res_id);
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
   * @return true if the machine's rack no longer contains machines
   */
  virtual bool RemoveMachine(ResourceID_t machine_res_id);

  inline const unordered_set<ResourceID_t, boost::hash<ResourceID_t>>&
    GetMachinesInRack(EquivClass_t rack_ec) {
    auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
    CHECK_NOTNULL(machines_in_rack);
    return *machines_in_rack;
  }
  inline uint64_t GetNumRacks() {
    return rack_to_machine_res_.size();
  }
  inline void GetRackIDs(vector<EquivClass_t>* rack_ids) {
    for (auto& rack_to_machines : rack_to_machine_res_) {
      rack_ids->push_back(rack_to_machines.first);
    }
  }
  inline EquivClass_t GetRackForMachine(ResourceID_t machine_res_id) {
    EquivClass_t* rack_ec =
      FindOrNull(machine_to_rack_ec_, machine_res_id);
    CHECK_NOTNULL(rack_ec);
    return *rack_ec;
  }

 private:
  // Set storing the racks to which we can still connect machines.
  unordered_set<EquivClass_t> racks_with_spare_links_;
  // Map storing the machine resource ids associated with each rack.
  unordered_map<EquivClass_t,
    unordered_set<ResourceID_t, boost::hash<ResourceID_t>>>
    rack_to_machine_res_;
  // Map storing the rack EC associated with each machine.
  unordered_map<ResourceID_t, EquivClass_t, boost::hash<ResourceID_t>>
    machine_to_rack_ec_;
  // Counter used to generate unique rack ids.
  EquivClass_t unique_rack_id_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DFS_H
