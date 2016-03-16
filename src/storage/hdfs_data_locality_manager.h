// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H
#define FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H

#include <hdfs.h>
#include <string>
#include <vector>

#include "base/common.h"
#include "scheduling/data_layer_manager_interface.h"

namespace firmament {
namespace store {

class HdfsDataLocalityManager : public DataLayerManagerInterface {
 public:
  HdfsDataLocalityManager();
  virtual ~HdfsDataLocalityManager();

  /**
   * Returns the locations of a block.
   * @param filename the file of which the block is part of
   * @param block_index the index of the block within the file
   * @return a vector of strings representing locations
   */
  vector<string> GetBlockLocations(const string& filename, int32_t block_index);

  /**
   * Returns the locations of all the blocks of a file.
   * @param filename the file for which to return locations
   * @return a containing a vector of locations for every block
   */
  vector<vector<string> > GetFileBlockLocations(const string& filename);

  /**
   * Returns the number of blocks a file has.
   * @param filename the file for which to return the number of blocks
   * @return number of blocks
   */
  uint32_t GetNumberOfBlocks(const string& filename);

 private:
  hdfsFS fs_;
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_STORAGE_HDFS_DATA_LOCALITY_MANAGER_H
