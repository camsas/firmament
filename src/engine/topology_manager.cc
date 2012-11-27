// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Implementation of the topology manager, gathering machine topology
// information via hwloc, and exposing it using a variety of interfaces.

#include "engine/topology_manager.h"

#include <vector>

#include "misc/utils.h"

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
             << "topology generation. Version is " << hwloc_get_api_version()
             << ", we require >=1.5. Topology string was: " << topology_desc;
#endif
}

vector<ResourceDescriptor> TopologyManager::FlatResourceSet() {
  // N.B.: This only returns resources corresponding to the CPU cores available
  // on the machine, and does not relate them in any kind of hierarchy.
  // It only returns the leaf nodes in the resource tree.
  vector<ResourceDescriptor> rds;
  for (uint32_t i = 0; i < NumProcessingUnits(); ++i) {
    ResourceID_t rid = GenerateUUID();
    ResourceDescriptor rd;
    rd.set_uuid(to_string(rid));
    rd.set_state(ResourceDescriptor::RESOURCE_IDLE);
    rd.set_friendly_name("Logical (OS) CPU core " + to_string(i));
    rds.push_back(rd);
  }
  return rds;
}

void TopologyManager::MakeProtobufTree(
    hwloc_obj_t node,
    ResourceTopologyNodeDescriptor* obj_pb,
    ResourceTopologyNodeDescriptor* parent_pb) {
  char obj_string[128];
  // Add this object
  hwloc_obj_snprintf(obj_string, sizeof(obj_string), topology_, node, " #", 0);
  string obj_id = to_string(GenerateUUID());
  obj_pb->mutable_resource_desc()->set_uuid(to_string(obj_id));
  obj_pb->mutable_resource_desc()->set_type(TranslateHwlocType(node->type));
  obj_pb->mutable_resource_desc()->set_friendly_name(obj_string);
  // If we have a parent_pb, also add this object's ID to the parent object's
  // resource descriptor
  if (parent_pb)
    parent_pb->mutable_resource_desc()->add_children(obj_id);
  // Iterate over the children of this object and add them recursively
  hwloc_obj_t prev_child_obj = NULL;
  for (uint32_t depth = 0; depth < node->arity; depth++) {
    hwloc_obj_t next_child_obj = hwloc_get_next_child(topology_, node,
                                                      prev_child_obj);
    ResourceTopologyNodeDescriptor* child = obj_pb->add_children();
    obj_pb->set_parent_id(obj_id);
    MakeProtobufTree(next_child_obj, child, obj_pb);
    prev_child_obj = next_child_obj;
  }
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

ResourceDescriptor::ResourceType TopologyManager::TranslateHwlocType(
    hwloc_obj_type_t obj_type) {
  switch (obj_type) {
    case HWLOC_OBJ_MACHINE:
      return ResourceDescriptor::RESOURCE_MACHINE;
    case HWLOC_OBJ_SOCKET:
      return ResourceDescriptor::RESOURCE_SOCKET;
    case HWLOC_OBJ_NODE:
      return ResourceDescriptor::RESOURCE_NUMA_NODE;
    case HWLOC_OBJ_CACHE:
      return ResourceDescriptor::RESOURCE_CACHE;
    case HWLOC_OBJ_CORE:
      return ResourceDescriptor::RESOURCE_CORE;
    case HWLOC_OBJ_PU:
      return ResourceDescriptor::RESOURCE_PU;
    default:
      LOG(FATAL) << "Unknown hwloc object type " << obj_type
                 << " encountered!";
  }
}

void TopologyManager::TraverseProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(const ResourceDescriptor&)> callback) {
  VLOG(1) << "Invoking TPT callback for resource "
          << pb->resource_desc().uuid();
  callback(pb->resource_desc());
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
       rtnd_iter = pb->mutable_children()->begin();
       rtnd_iter != pb->mutable_children()->end();
       ++rtnd_iter) {
    TraverseProtobufTree(&(*rtnd_iter), callback);
  }
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
