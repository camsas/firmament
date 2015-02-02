// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Utility functions for working with protobufs.

#include <queue>

#include "misc/pb_utils.h"

namespace firmament {

// Overload taking a callback that itself takes a ResourceDescriptor as its
// argument.
void DFSTraverseResourceProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb->mutable_resource_desc());
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
       rtnd_iter = pb->mutable_children()->begin();
       rtnd_iter != pb->mutable_children()->end();
       ++rtnd_iter) {
    BFSTraverseResourceProtobufTree(&(*rtnd_iter), callback);
  }
}

// Overload taking a callback that itself takes a ResourceTopologyNodeDescriptor
// as its argument.
void DFSTraverseResourceProtobufTreeReturnRTND(
    const ResourceTopologyNodeDescriptor& pb,
    boost::function<void(const ResourceTopologyNodeDescriptor&)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb.resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb);
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator
       rtnd_iter = pb.children().begin();
       rtnd_iter != pb.children().end();
       ++rtnd_iter) {
    DFSTraverseResourceProtobufTreeReturnRTND((*rtnd_iter), callback);
  }
}

// Overload taking a callback that itself takes a ResourceTopologyNodeDescriptor
// as its argument.
void DFSTraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb);
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
       rtnd_iter = pb->mutable_children()->begin();
       rtnd_iter != pb->mutable_children()->end();
       ++rtnd_iter) {
    DFSTraverseResourceProtobufTreeReturnRTND(&(*rtnd_iter), callback);
  }
}

void BFSTraverseResourceProtobufTree(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "BFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  queue<ResourceTopologyNodeDescriptor*> to_visit;
  to_visit.push(pb);
  while (!to_visit.empty()) {
    ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
    to_visit.pop();
    callback(res_node_desc->mutable_resource_desc());
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::iterator
         rtnd_iter = res_node_desc->mutable_children()->begin();
         rtnd_iter != res_node_desc->mutable_children()->end();
         ++rtnd_iter) {
      to_visit.push(&(*rtnd_iter));
    }
  }
}

void BFSTraverseResourceProtobufTreeReturnRTND(
    const ResourceTopologyNodeDescriptor* pb,
    boost::function<void(const ResourceTopologyNodeDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "BFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  queue<const ResourceTopologyNodeDescriptor*> to_visit;
  to_visit.push(pb);
  while (!to_visit.empty()) {
    const ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
    to_visit.pop();
    callback(res_node_desc);
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator
         rtnd_iter = res_node_desc->children().begin();
         rtnd_iter != res_node_desc->children().end();
         ++rtnd_iter) {
      to_visit.push(&(*rtnd_iter));
    }
  }
}

}  // namespace firmament
