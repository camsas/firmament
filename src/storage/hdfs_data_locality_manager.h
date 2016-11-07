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

#ifndef FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H
#define FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H

#include <hdfs.h>
#include <string>
#include <vector>

#include "scheduling/data_layer_manager_interface.h"

#include "base/common.h"
#include "misc/trace_generator.h"

DECLARE_bool(enable_hdfs_data_locality);

namespace firmament {
namespace store {

class HdfsDataLocalityManager : public DataLayerManagerInterface {
 public:
  HdfsDataLocalityManager(TraceGenerator* trace_generator);
  virtual ~HdfsDataLocalityManager();

  EquivClass_t AddMachine(const string& hostname, ResourceID_t res_id);
  /**
   * Returns the locations of all the blocks of a file.
   * @param filename the file for which to return locations
   * @param a pointer to a list to which the locations should be added
   */
  void GetFileLocations(const string& file_path, list<DataLocation>* locations);
  int64_t GetFileSize(const string& filename);
  bool RemoveMachine(const string& hostname);

  const unordered_set<ResourceID_t, boost::hash<ResourceID_t>>&
    GetMachinesInRack(EquivClass_t rack_ec) {
    CHECK_EQ(rack_ec, 1);
    return machines_;
  }
  inline uint64_t GetNumRacks() {
    // TODO(ionel): Implement!
    return 1;
  }
  inline void GetRackIDs(vector<EquivClass_t>* rack_ids) {
    // TODO(ionel): Implement!
    rack_ids->push_back(1);
  }
  inline EquivClass_t GetRackForMachine(ResourceID_t machine_res_id) {
    return 1;
  }

 private:
  uint64_t GenerateBlockID(const string& file_path, int32_t block_index);
  /**
   * Returns the locations of a block.
   * @param filename the file of which the block is part of
   * @param block_index the index of the block within the file
   * @return a vector of strings representing locations
   */
  vector<string> GetBlockLocations(const string& filename, int32_t block_index);

  /**
   * Returns the number of blocks a file has.
   * @param filename the file for which to return the number of blocks
   * @return number of blocks
   */
  uint32_t GetNumberOfBlocks(const string& filename);
  ResourceID_t HostToResourceID(const string& hostname);

 private:
  hdfsFS fs_;
  unordered_map<string, ResourceID_t> hostname_to_res_id_;
  unordered_set<ResourceID_t, boost::hash<ResourceID_t>> machines_;
  TraceGenerator* trace_generator_;
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H
