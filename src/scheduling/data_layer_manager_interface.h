// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H
#define FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H

#include <list>

#include "base/types.h"

namespace firmament {

struct DataLocation {
  DataLocation(ResourceID_t machine_res_id, uint64_t block_id,
               uint64_t size_bytes) : machine_res_id_(machine_res_id),
    block_id_(block_id), size_bytes_(size_bytes) {
  }
  ResourceID_t machine_res_id_;
  uint64_t block_id_;
  uint64_t size_bytes_;
};

class DataLayerManagerInterface {
 public:
  virtual void AddMachine(const string& hostname, ResourceID_t res_id) = 0;
  virtual list<DataLocation> GetFileLocations(const string& file_path) = 0;
  virtual void RemoveMachine(const string& hostname) = 0;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H
