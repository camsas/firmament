// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of the topology manager, gathering machine topology
// information via hwloc, and exposing it using a variety of interfaces.

#include "engine/topology_manager.h"

namespace firmament {
namespace machine {
namespace topology {

TopologyManager::TopologyManager() {
  hwloc_topology_init(&topology_);
  VLOG(1) << "Topology manager initialized.";

  LoadAndParseTopology();
}

void TopologyManager::AsProtobuf(ResourceDescriptor* topology_pb) {
  // TODO(malte): stub
  ResourceDescriptor root_resource;
#if 0
  root_resource.set_type(RESOURCE_MACHINE);
  for (uint32_t depth = 0; depth < topology_depth_; depth++) {
    uint32_t num_objs_at_level = hwloc_get_nbobjs_by_depth(topology_, depth);
    for (uint32_t i = 0; i < num_objs_at_level; ++i) {
      hwloc_obj_snprintf(obj_string, sizeof(obj_string), topology_,
                         hwloc_get_obj_by_depth(topology_, depth, i), "#", 0);
      LOG(INFO) << "Index: " << i << ": " << obj_string;
    }
  }
#endif
}

void TopologyManager::LoadAndParseTopology() {
  VLOG(1) << "Analyzing machine topology...";
  // library call to perform topology detection
  hwloc_topology_load(topology_);
  topology_depth_ = hwloc_topology_get_depth(topology_);
}

void TopologyManager::LoadAndParseSyntheticTopology(
    const string& topology_desc) {
  VLOG(1) << "Synthetic topology load...";
#if HWLOC_API_VERSION > 0x00010500
  hwloc_topology_set_synthetic(topology_, topology_desc);
  hwloc_topology_load(topology_);
  topology_depth_ = hwloc_topology_get_depth(topology_);
#else
  LOG(ERROR) << "The version of hwloc used is too old to support synthetic "
             << "topology generation. Version is " << hwloc_get_api_version
             << ", we require >=1.5. Topology string was: " << topology_desc;
#endif
}

uint32_t TopologyManager::NumProcessingUnits() {
  hwloc_obj_t obj = NULL;
  uint32_t count = 0;
  do {
    obj = hwloc_get_next_obj_by_type(topology_, HWLOC_OBJ_PU, obj);
    if (obj != NULL)
      count++;
  } while (obj != NULL);
  return count;
}

void TopologyManager::DebugPrintRawTopology() {
  char obj_string[128];
  for (uint32_t depth = 0; depth < topology_depth_; depth++) {
    LOG(INFO) << "*** LEVEL: " << depth;
    for (uint32_t i = 0; i < hwloc_get_nbobjs_by_depth(topology_, depth);
         ++i) {
      hwloc_obj_snprintf(obj_string, sizeof(obj_string), topology_,
                         hwloc_get_obj_by_depth(topology_, depth, i), "#", 0);
      LOG(INFO) << "Index: " << i << ": " << obj_string;
    }
  }
}

}  // namespace topology
}  // namespace machine
}  // namespace firmament
