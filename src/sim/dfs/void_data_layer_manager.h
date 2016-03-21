// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SIM_DFS_VOID_DATA_LAYER_MANAGER_H
#define FIRMAMENT_SIM_DFS_VOID_DATA_LAYER_MANAGER_H

#include "sim/dfs/simulated_data_layer_manager.h"

namespace firmament {
namespace sim {

class VoidDataLayerManager : public SimulatedDataLayerManager {
 public:
  uint64_t AddFilesForTask(TaskID_t task_id, uint64_t avg_runtime);
  void AddMachine(const string& hostname, ResourceID_t machine_res_id);
  void GetFileLocations(const string& file_path, list<DataLocation>* locations);
  void RemoveFilesForTask(TaskID_t task_id);
  void RemoveMachine(const string& hostname);
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_VOID_DATA_LAYER_MANAGER_H
