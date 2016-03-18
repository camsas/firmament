// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "storage/hdfs_data_locality_manager.h"

#include <hdfs.h>
#include <string>
#include <vector>

DEFINE_string(hdfs_name_node_address, "hdfs://localhost",
              "The address of the HDFS name node");
DEFINE_int32(hdfs_name_node_port, 8020,
             "The port of the HDFS name node");

namespace firmament {
namespace store {

HdfsDataLocalityManager::HdfsDataLocalityManager() {
  struct hdfsBuilder* hdfs_builder = hdfsNewBuilder();
  if (!hdfs_builder) {
    LOG(FATAL) << "Could not create HDFS builder";
  }
  hdfsBuilderSetNameNode(hdfs_builder, FLAGS_hdfs_name_node_address.c_str());
  hdfsBuilderSetNameNodePort(hdfs_builder, FLAGS_hdfs_name_node_port);
  fs_ = hdfsBuilderConnect(hdfs_builder);
  hdfsFreeBuilder(hdfs_builder);
  if (!fs_) {
    LOG(FATAL) << "Could not connect to HDFS";
  }
}

HdfsDataLocalityManager::~HdfsDataLocalityManager() {
  hdfsDisconnect(fs_);
}

vector<string> HdfsDataLocalityManager::GetBlockLocations(
    const string& filename,
    int32_t block_index) {
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, filename.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << filename;
    return vector<string>();
  }
  tOffset file_size = file_stat->mSize;
  int num_blocks = 0;
  BlockLocation* block_location =
    hdfsGetFileBlockLocations(fs_, filename.c_str(), 0, file_size, &num_blocks);
  if (!block_location) {
    LOG(ERROR) << "Could not get HDFS block locations for: " << filename;
    return vector<string>();
  }
  if (block_index >= num_blocks) {
    LOG(ERROR) << "Block index " << block_index << " for file " << filename
               << " is invalid";
  }
  vector<string> locations;
  for (int32_t repl_index = 0;
       repl_index < block_location[block_index].numOfNodes;
       ++repl_index) {
    locations.push_back(block_location[block_index].hosts[repl_index]);
  }
  hdfsFreeFileInfo(file_stat, 1);
  hdfsFreeFileBlockLocations(block_location, num_blocks);
  return locations;
}

vector<vector<string> > HdfsDataLocalityManager::GetFileBlockLocations(
    const string& filename) {
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, filename.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << filename;
    return vector<vector<string> >();
  }
  tOffset file_size = file_stat->mSize;
  int num_blocks = 0;
  BlockLocation* block_location =
    hdfsGetFileBlockLocations(fs_, filename.c_str(), 0, file_size, &num_blocks);
  if (!block_location) {
    LOG(ERROR) << "Could not get HDFS block locations for: " << filename;
    return vector<vector<string> >();
  }
  vector<vector<string> > locations(num_blocks);
  for (int32_t block_index = 0; block_index < num_blocks; ++block_index) {
    for (int32_t repl_index = 0;
         repl_index < block_location[block_index].numOfNodes;
         ++repl_index) {
      locations[block_index].push_back(
          block_location[block_index].hosts[repl_index]);
    }
  }
  hdfsFreeFileInfo(file_stat, 1);
  hdfsFreeFileBlockLocations(block_location, num_blocks);
  return locations;
}

list<DataLocation> HdfsDataLocalityManager::GetFileLocations() {
  // TODO(ionel): Implement!
  list<DataLocation> locations;
  return locations;
}

uint32_t HdfsDataLocalityManager::GetNumberOfBlocks(const string& filename) {
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, filename.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << filename;
    return 0;
  }
  tOffset file_size = file_stat->mSize;
  int num_blocks = 0;
  BlockLocation* block_location =
    hdfsGetFileBlockLocations(fs_, filename.c_str(), 0, file_size, &num_blocks);
  if (!block_location) {
    LOG(ERROR) << "Could not get HDFS block locations for: " << filename;
    return 0;
  }
  hdfsFreeFileInfo(file_stat, 1);
  hdfsFreeFileBlockLocations(block_location, num_blocks);
  return num_blocks;
}

} // namespace store
} // namespace firmament
