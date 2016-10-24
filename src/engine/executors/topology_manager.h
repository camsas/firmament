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

// Machine topology manager. This class is responsible for gathering machine
// topology information (interfacing with the hwloc libraries), and allows it to
// be queried in various convenient ways.
// It also implements export hooks that allow this information to be
// communicated to other parts of the system (e.g. coordinators).o
//
// hwloc is under BSD license, permitting use and redistribution.

#ifndef FIRMAMENT_ENGINE_EXECUTORS_TOPOLOGY_MANAGER_H
#define FIRMAMENT_ENGINE_EXECUTORS_TOPOLOGY_MANAGER_H

#include <string>
#include <map>
#include <vector>

extern "C" {
#include <hwloc.h>
}

#include "base/common.h"
#include "base/types.h"
#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"

namespace firmament {
namespace machine {
namespace topology {

class TopologyManager {
 public:
  TopologyManager();
  void AsProtobuf(ResourceTopologyNodeDescriptor* topology_pb);
  void AsProtobuf(hwloc_topology_t topology,
                  ResourceTopologyNodeDescriptor* topology_pb);
  bool BindPIDToResource(pid_t pid, ResourceID_t res_id);
  bool BindSelfToResource(ResourceID_t res_id);
  vector<ResourceDescriptor> FlatResourceSet();
  void LoadAndParseTopology();
  uint32_t LoadAndParseSyntheticTopology(const string& topology_desc,
                                         hwloc_topology_t topology);
  void DebugPrintRawTopology();
  uint32_t NumProcessingUnits() const;

 protected:
  string DebugCPUSet(hwloc_const_cpuset_t cpuset);
  void MakeProtobufTree(hwloc_topology_t topology,
                        hwloc_obj_t node,
                        ResourceTopologyNodeDescriptor* obj_pb,
                        ResourceTopologyNodeDescriptor* parent_pb);
  ResourceDescriptor::ResourceType TranslateHwlocType(
      hwloc_obj_type_t obj_type) const;
  // Local fields holding topology information
  hwloc_topology_t topology_;
  hwloc_cpuset_t cpuset_;
  uint32_t topology_depth_;
  map<hwloc_obj_t, ResourceID_t> obj_to_resourceID_;
  map<ResourceID_t, hwloc_obj_t> resourceID_to_obj_;
};

}  // namespace topology
}  // namespace machine
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_EXECUTORS_TOPOLOGY_MANAGER_H
