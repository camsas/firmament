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

template <typename T>
bool RepeatedContainsPtr(RepeatedPtrField<T>* pbf, T* item) {
  // N.B.: using GNU-style RTTI
  for (typeof(pbf->pointer_begin()) iter =
       pbf->pointer_begin();
       iter != pbf->pointer_end();
       ++iter) {
    if (*iter == item)
      return true;
  }
  return false;
}

// Overload taking a callback that itself takes a ResourceDescriptor as its
// argument.
void TraverseResourceProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "Traversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb->mutable_resource_desc());
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
       rtnd_iter = pb->mutable_children()->begin();
       rtnd_iter != pb->mutable_children()->end();
       ++rtnd_iter) {
    TraverseResourceProtobufTree(&(*rtnd_iter), callback);
  }
}

// Overload taking a callback that itself takes a ResourceTopologyNodeDescriptor
// as its argument.
void TraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "Traversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb);
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
       rtnd_iter = pb->mutable_children()->begin();
       rtnd_iter != pb->mutable_children()->end();
       ++rtnd_iter) {
    TraverseResourceProtobufTreeReturnRTND(&(*rtnd_iter), callback);
  }
}

}  // namespace firmament

#endif  // FIRMAMENT_MISC_PB_UTILS_H
