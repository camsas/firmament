// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H
#define FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H

#include <hdfs.h>
#include <string>
#include <vector>

#include "scheduling/data_layer_manager_interface.h"

#include "base/common.h"
#include "misc/trace_generator.h"

namespace firmament {
namespace store {

class HdfsDataLocalityManager : public DataLayerManagerInterface {
 public:
  HdfsDataLocalityManager(TraceGenerator* trace_generator);
  virtual ~HdfsDataLocalityManager();

  void AddMachine(const string& hostname, ResourceID_t res_id);
  /**
   * Returns the locations of all the blocks of a file.
   * @param filename the file for which to return locations
   * @param a pointer to a list to which the locations should be added
   */
  void GetFileLocations(const string& file_path, list<DataLocation>* locations);
  void RemoveMachine(const string& hostname);

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
  TraceGenerator* trace_generator_;
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H