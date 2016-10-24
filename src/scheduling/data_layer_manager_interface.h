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
  virtual int64_t GetFileSize(const string& file_path) = 0;
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
