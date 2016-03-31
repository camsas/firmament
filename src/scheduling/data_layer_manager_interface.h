// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H
#define FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H

#include <list>

#include "base/types.h"

namespace firmament {

struct DataLocation {
  DataLocation(ResourceID_t machine_res_id, EquivClass_t rack_id,
               uint64_t block_id, uint64_t size_bytes)
  : machine_res_id_(machine_res_id), rack_id_(rack_id),
    block_id_(block_id), size_bytes_(size_bytes) {
  }
  ResourceID_t machine_res_id_;
  EquivClass_t rack_id_;
  uint64_t block_id_;
  uint64_t size_bytes_;
};

class DataLayerManagerInterface {
 public:
  virtual EquivClass_t AddMachine(const string& hostname,
                                  ResourceID_t res_id) = 0;
  virtual void GetFileLocations(const string& file_path,
                                list<DataLocation>* locations) = 0;
  virtual const unordered_set<ResourceID_t, boost::hash<ResourceID_t>>&
    GetMachinesInRack(EquivClass_t rack_ec) = 0;
  virtual uint64_t GetNumRacks() = 0;
  virtual void GetRackIDs(vector<EquivClass_t>* rack_ids) = 0;
  virtual EquivClass_t GetRackForMachine(ResourceID_t machine_res_id) = 0;
  /**
   * Removes a machine from the data layer.
   * @param hostname of the machine to be removed
   * @return true if the machine was the last one in its rack
   */
  virtual bool RemoveMachine(const string& hostname) = 0;
};

} // namespace firmament

#endif // FIRMAMENT_SCHEDULING_DATA_LAYER_MANAGER_INTERFACE_H
