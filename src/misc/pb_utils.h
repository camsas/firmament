// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility functions for working with protobufs.

#ifndef FIRMAMENT_MISC_PB_UTILS_H
#define FIRMAMENT_MISC_PB_UTILS_H

#include <string>

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"

namespace firmament {

// Traversal implementation taking a callback that itself takes a
// ResourceDescriptor as its argument.
void DFSTraverseResourceProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceDescriptor*)> callback);  // NOLINT

// Traversal implementation taking a callback that itself takes a
// ResourceTopologyNodeDescriptor as its argument.
void DFSTraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback);  // NOLINT

// Traversal implementation taking a callback that itself takes a
// ResourceTopologyNodeDescriptor as its argument.
void DFSTraverseResourceProtobufTreeReturnRTND(
    const ResourceTopologyNodeDescriptor& pb,
    boost::function<void(const ResourceTopologyNodeDescriptor&)> callback);  // NOLINT

void BFSTraverseResourceProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceDescriptor*)> callback);  // NOLINT

void BFSTraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback);  // NOLINT

void BFSTraverseResourceProtobufTreeToHash(
    ResourceTopologyNodeDescriptor* pb, size_t* hash,
    boost::function<void(ResourceTopologyNodeDescriptor*, size_t*)> callback);  // NOLINT

template <typename T>
bool RepeatedContainsPtr(RepeatedPtrField<T>* pbf, T* item) {
  // N.B.: using GNU-style RTTI
  for (__typeof__(pbf->pointer_begin()) iter =
       pbf->pointer_begin();
       iter != pbf->pointer_end();
       ++iter) {
    if (*iter == item)
      return true;
  }
  return false;
}

}  // namespace firmament

#endif  // FIRMAMENT_MISC_PB_UTILS_H
