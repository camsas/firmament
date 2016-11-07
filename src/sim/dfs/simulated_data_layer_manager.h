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

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H

#include "scheduling/data_layer_manager_interface.h"

#include "misc/trace_generator.h"
#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"
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
  int64_t GetFileSize(const string& file_path);
  void RemoveFilesForTask(const TaskDescriptor& td);
  bool RemoveMachine(const string& hostname);
  inline const unordered_set<ResourceID_t, boost::hash<ResourceID_t>>&
    GetMachinesInRack(EquivClass_t rack_ec) {
    return dfs_->GetMachinesInRack(rack_ec);
  }
  inline uint64_t GetNumRacks() {
    return dfs_->GetNumRacks();
  }
  inline void GetRackIDs(vector<EquivClass_t>* rack_ids) {
    return dfs_->GetRackIDs(rack_ids);
  }
  inline EquivClass_t GetRackForMachine(ResourceID_t machine_res_id) {
    return dfs_->GetRackForMachine(machine_res_id);
  }

 private:
  GoogleBlockDistribution* input_block_dist_;
  GoogleRuntimeDistribution* runtime_dist_;
  SimulatedDFS* dfs_;
  unordered_map<string, ResourceID_t> hostname_to_res_id_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
