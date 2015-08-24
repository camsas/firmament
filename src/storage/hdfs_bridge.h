// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#ifndef FIRMAMENT_STORAGE_HDFS_BRIDGE_H
#define FIRMAMENT_STORAGE_HDFS_BRIDGE_H

#include <string>
#include <vector>

#include "base/common.h"

namespace firmament {
namespace store {

class HdfsBridge {
 public:
  HdfsBridge();
  virtual ~HdfsBridge();

  vector<string> GetBlockLocations(const char* filename, int32_t block_index);
  vector<vector<string> > GetFileBlockLocations(const char* filename);
  uint32_t GetNumberOfBlocks(const char* filename);

 private:
  hdfsFS fs_;
};

} // namespace store
} // namespace firmament

#endif  // FIRMAMENT_STORAGE_HDFS_BRIDGE_H
