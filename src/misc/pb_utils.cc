// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility functions for working with protobufs.

#include "misc/pb_utils.h"

namespace firmament {

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
