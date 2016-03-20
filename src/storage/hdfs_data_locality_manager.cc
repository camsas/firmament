// The Firmament project
// Copyright (c) 2015 Ionel Gog <ionel.gog@cl.cam.ac.uk>

#include "storage/hdfs_data_locality_manager.h"

#include <hdfs.h>
#include <string>
#include <vector>

#include "misc/utils.h"
#include "misc/map-util.h"

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

uint64_t HdfsDataLocalityManager::GenerateBlockID(const string& file_path,
                                                  int32_t block_index) {
  uint64_t path_hash = HashString(file_path);
  boost::hash_combine(path_hash, block_index);
  return path_hash;
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

list<DataLocation> HdfsDataLocalityManager::GetFileLocations(
    const string& file_path) {
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, file_path.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << file_path;
    return list<DataLocation>();
  }
  tOffset file_size = file_stat->mSize;
  int num_blocks = 0;
  BlockLocation* block_location =
    hdfsGetFileBlockLocations(fs_, file_path.c_str(), 0, file_size, &num_blocks);
  if (!block_location) {
    LOG(ERROR) << "Could not get HDFS block locations for: " << file_path;
    return list<DataLocation>();
  }
  list<DataLocation> locations;
  for (int32_t block_index = 0; block_index < num_blocks; ++block_index) {
    for (int32_t repl_index = 0;
         repl_index < block_location[block_index].numOfNodes;
         ++repl_index) {
      ResourceID_t machine_res_id =
          HostToResourceID(block_location[block_index].hosts[repl_index]);
      uint64_t block_id = GenerateBlockID(file_path, block_index);
      int64_t block_size = block_location[block_index].length;
      DataLocation data_location(machine_res_id, block_id,
                                 static_cast<uint64_t>(block_size));
      locations.push_back(data_location);
    }
  }
  hdfsFreeFileInfo(file_stat, 1);
  hdfsFreeFileBlockLocations(block_location, num_blocks);
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

ResourceID_t HdfsDataLocalityManager::HostToResourceID(const string& hostname) {
  ResourceID_t* res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(res_id);
  return *res_id;
}

void HdfsDataLocalityManager::AddMachine(const string& hostname,
                                         ResourceID_t res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, res_id));
}

void HdfsDataLocalityManager::RemoveMachine(const string& hostname) {
  hostname_to_res_id_.erase(hostname);
}

} // namespace store
} // namespace firmament
