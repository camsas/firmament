// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H

#include "scheduling/data_layer_manager_interface.h"

#include "misc/trace_generator.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs_interface.h"
#include "sim/google_runtime_distribution.h"

namespace firmament {
namespace sim {

class SimulatedDataLayerManager : public DataLayerManagerInterface {
 public:
  SimulatedDataLayerManager(TraceGenerator* trace_generator);
  virtual ~SimulatedDataLayerManager();

  /**
   * Add files for a given task.
   * @param td the descriptor of the task
   * @param avg_runtime the average runtime of the task
   * @param long_running_service true if the task doesn't finish in the trace
   * @param max_machine_spread the maximum number of machines over which
   * the task's inputs should be spread.
   */
  uint64_t AddFilesForTask(const TaskDescriptor& td, uint64_t avg_runtime,
                           bool long_running_service,
                           uint64_t max_machine_spread);
  EquivClass_t AddMachine(const string& hostname, ResourceID_t machine_res_id);
  void GetFileLocations(const string& file_path, list<DataLocation>* locations);
  void RemoveFilesForTask(const TaskDescriptor& td);
  bool RemoveMachine(const string& hostname);
  inline const unordered_set<ResourceID_t, boost::hash<ResourceID_t>>&
    GetMachinesInRack(EquivClass_t rack_ec) {
    auto machines_in_rack = FindOrNull(rack_to_machine_res_, rack_ec);
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
  GoogleBlockDistribution* input_block_dist_;
  GoogleRuntimeDistribution* runtime_dist_;
  SimulatedDFSInterface* dfs_;
  unordered_map<string, ResourceID_t> hostname_to_res_id_;
  TraceGenerator* trace_generator_;
  // Counter used to generate unique rack ids.
  EquivClass_t unique_rack_id_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
