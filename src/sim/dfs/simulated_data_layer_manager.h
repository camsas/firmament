// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H

#include "scheduling/data_layer_manager_interface.h"

#include "sim/dfs/google_block_distribution.h"
#include "sim/dfs/simulated_dfs.h"
#include "sim/google_runtime_distribution.h"

namespace firmament {
namespace sim {

class SimulatedDataLayerManager : public DataLayerManagerInterface {
 public:
  SimulatedDataLayerManager();
  virtual ~SimulatedDataLayerManager();
  list<DataLocation> GetFileLocations();

 private:
  GoogleBlockDistribution* input_block_dist_;
  GoogleBlockDistribution* file_block_dist_;
  GoogleRuntimeDistribution* runtime_dist_;
  SimulatedDFS* dfs_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DATA_LAYER_MANAGER_H
