// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Machine topology manager. This class is responsible for gathering machine
// topology information (interfacing with the hwloc libraries), and allows it to
// be queried in various convenient ways.
// It also implements export hooks that allow this information to be
// communicated to other parts of the system (e.g. coordinators).o
//
// hwloc is under BSD license, permitting use and redistribution.

#ifndef FIRMAMENT_ENGINE_TOPOLOGY_MANAGER_H
#define FIRMAMENT_ENGINE_TOPOLOGY_MANAGER_H

#include <string>
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
  bool BindToCore(uint32_t core_id, bool strict);
  bool BindToCPUMask(uint64_t mask, bool strict);
  bool BindToResource(ResourceID_t res_id);
  vector<ResourceDescriptor> FlatResourceSet();
  void LoadAndParseTopology();
  void LoadAndParseSyntheticTopology(const string& topology_desc);
  void DebugPrintRawTopology();
  uint32_t NumProcessingUnits();
  void TraverseProtobufTree(
      ResourceTopologyNodeDescriptor* pb,
      boost::function<void(ResourceDescriptor*)> callback);  // NOLINT

 protected:
  void MakeProtobufTree(hwloc_obj_t node,
                        ResourceTopologyNodeDescriptor* obj_pb,
                        ResourceTopologyNodeDescriptor* parent_pb);
  ResourceDescriptor::ResourceType TranslateHwlocType(
      hwloc_obj_type_t obj_type);
  // Local fields holding topology information
  hwloc_topology_t topology_;
  hwloc_cpuset_t cpuset_;
  uint32_t topology_depth_;
};

}  // namespace topology
}  // namespace machine
}  // namespace firmament

#endif  // FIRMAMENT_ENGINE_COORDINATOR_H
