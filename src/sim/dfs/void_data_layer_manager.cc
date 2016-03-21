// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/dfs/void_data_layer_manager.h"

namespace firmament {
namespace sim {

uint64_t VoidDataLayerManager::AddFilesForTask(TaskID_t task_id,
                                               uint64_t avg_runtime) {
  return 0;
}

void VoidDataLayerManager::AddMachine(const string& hostname,
                                      ResourceID_t machine_res_id) {
}

void VoidDataLayerManager::GetFileLocations(const string& file_path,
                                            list<DataLocation>* locations) {
}

void VoidDataLayerManager::RemoveFilesForTask(TaskID_t task_id) {
}

void VoidDataLayerManager::RemoveMachine(const string& hostname) {
}

} // namespace sim
} // namespace firmament
