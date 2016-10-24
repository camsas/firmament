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

#include "storage/hdfs_data_locality_manager.h"

#include <hdfs.h>
#include <string>
#include <vector>

#include "misc/utils.h"
#include "misc/map-util.h"
#include "scheduling/flow/cost_model_interface.h"
#include "scheduling/flow/flow_scheduler.h"

DEFINE_bool(enable_hdfs_data_locality, false,
            "True if the scheduler should consider input file block locality "
            "in HDFS.");
DEFINE_string(hdfs_name_node_address, "hdfs://localhost",
              "The address of the HDFS name node");
DEFINE_int32(hdfs_name_node_port, 8020,
             "The port of the HDFS name node");

static bool ValidateHDFSDataLocality(const char* flagname, bool enable_dl) {
#ifdef ENABLE_HDFS
   if (!enable_dl) {
     // Quincy cost model requires HDFS data locality to be enabled
     if (FLAGS_flow_scheduling_cost_model == firmament::COST_MODEL_QUINCY) {
       LOG(ERROR) << "To use the Quincy cost model, HDFS data locality must "
         << "be enabled.\n"
         << "Pass the --" << flagname << " argument and ensure that "
         << "the HDFS NameNode is correctly configured and reachable.";
       return false;
     }
   }
#endif
   return true;
}
static const bool hdfs_dl_validator =
    google::RegisterFlagValidator(&FLAGS_enable_hdfs_data_locality,
                                  &ValidateHDFSDataLocality);

namespace firmament {
namespace store {

HdfsDataLocalityManager::HdfsDataLocalityManager(
    TraceGenerator* trace_generator) : trace_generator_(trace_generator) {
  struct hdfsBuilder* hdfs_builder = hdfsNewBuilder();
  if (!hdfs_builder) {
    LOG(FATAL) << "Could not create HDFS builder";
  }
  hdfsBuilderSetNameNode(hdfs_builder, FLAGS_hdfs_name_node_address.c_str());
  hdfsBuilderSetNameNodePort(hdfs_builder, FLAGS_hdfs_name_node_port);
  fs_ = hdfsBuilderConnect(hdfs_builder);
  hdfsFreeBuilder(hdfs_builder);
  if (!fs_) {
    LOG(FATAL) << "Could not connect to HDFS NameNode at "
               << FLAGS_hdfs_name_node_address << ":"
               << FLAGS_hdfs_name_node_port;
  }
}

HdfsDataLocalityManager::~HdfsDataLocalityManager() {
  // trace_generator_ is not owned by HdfsDataLocalityManager.
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

void HdfsDataLocalityManager::GetFileLocations(const string& file_path,
                                               list<DataLocation>* locations) {
  CHECK_NOTNULL(locations);
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, file_path.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << file_path;
    return;
  }
  tOffset file_size = file_stat->mSize;
  int num_blocks = 0;
  BlockLocation* block_location =
    hdfsGetFileBlockLocations(fs_, file_path.c_str(), 0, file_size, &num_blocks);
  if (!block_location) {
    LOG(ERROR) << "Could not get HDFS block locations for: " << file_path;
    return;
  }
  for (int32_t block_index = 0; block_index < num_blocks; ++block_index) {
    for (int32_t repl_index = 0;
         repl_index < block_location[block_index].numOfNodes;
         ++repl_index) {
      ResourceID_t machine_res_id =
          HostToResourceID(block_location[block_index].hosts[repl_index]);
      uint64_t block_id = GenerateBlockID(file_path, block_index);
      int64_t block_size = block_location[block_index].length;
      // TODO(ionel): Make sure DataLocation's rack_id_ is set to a correct
      // value.
      DataLocation data_location(machine_res_id, 1, block_id,
                                 static_cast<uint64_t>(block_size));
      locations->push_back(data_location);
    }
  }
  hdfsFreeFileInfo(file_stat, 1);
  hdfsFreeFileBlockLocations(block_location, num_blocks);
}

int64_t HdfsDataLocalityManager::GetFileSize(const string& filename) {
  hdfsFileInfo* file_stat = hdfsGetPathInfo(fs_, filename.c_str());
  if (!file_stat) {
    LOG(ERROR) << "Could not get HDFS file info for: " << filename;
    return 0;
  }
  return file_stat->mSize;
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
  return static_cast<uint32_t>(num_blocks);
}

ResourceID_t HdfsDataLocalityManager::HostToResourceID(const string& hostname) {
  // XXX(ionel): HACK! Remove!
  string local_hostname = hostname;
  size_t start_position = local_hostname.find(".cl.cam.ac.uk");
  if (start_position != string::npos) {
    local_hostname.erase(start_position, 13);
  }
  ResourceID_t* res_id = FindOrNull(hostname_to_res_id_, local_hostname);
  CHECK_NOTNULL(res_id);
  return *res_id;
}

EquivClass_t HdfsDataLocalityManager::AddMachine(const string& hostname,
                                                 ResourceID_t res_id) {
  CHECK(InsertIfNotPresent(&hostname_to_res_id_, hostname, res_id));
  machines_.insert(res_id);
  // TODO(ionel): Return the correct rack equivalence class.
  return 1;
}

bool HdfsDataLocalityManager::RemoveMachine(const string& hostname) {
  ResourceID_t* machine_res_id = FindOrNull(hostname_to_res_id_, hostname);
  CHECK_NOTNULL(machine_res_id);
  ResourceID_t res_tmp = *machine_res_id;
  machines_.erase(res_tmp);
  hostname_to_res_id_.erase(hostname);
  return machines_.size() > 0 ? true : false;
}

} // namespace store
} // namespace firmament
