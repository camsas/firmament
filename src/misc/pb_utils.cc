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
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
       rtnd_iter = pb->mutable_children()->pointer_begin();
       rtnd_iter != pb->mutable_children()->pointer_end();
       ++rtnd_iter) {
    DFSTraverseResourceProtobufTree(*rtnd_iter, callback);
  }
}

bool DFSTraverseResourceProtobufTreeWhileTrue(
    const ResourceTopologyNodeDescriptor& pb,
    boost::function<bool(const ResourceDescriptor&)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb.resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  if (!callback(pb.resource_desc())) {
    return false;
  }
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator
       rtnd_iter = pb.children().begin();
       rtnd_iter != pb.children().end();
       ++rtnd_iter) {
    if (!DFSTraverseResourceProtobufTreeWhileTrue(*rtnd_iter, callback)) {
      return false;
    }
  }
  return true;
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
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
       rtnd_iter = pb->mutable_children()->pointer_begin();
       rtnd_iter != pb->mutable_children()->pointer_end();
       ++rtnd_iter) {
    DFSTraverseResourceProtobufTreeReturnRTND(*rtnd_iter, callback);
  }
}

void DFSTraverseResourceProtobufTreesReturnRTNDs(
    ResourceTopologyNodeDescriptor* pb1,
    const ResourceTopologyNodeDescriptor& pb2,
    boost::function<void(ResourceTopologyNodeDescriptor*,
                         const ResourceTopologyNodeDescriptor&)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb1->resource_desc().uuid() << " and "
          << pb2.resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  callback(pb1, pb2);
  RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
    rtnd_iter1 = pb1->mutable_children()->pointer_begin();
  RepeatedPtrField<ResourceTopologyNodeDescriptor>::const_iterator
    rtnd_iter2 = pb2.children().begin();
  for (; rtnd_iter1 != pb1->mutable_children()->pointer_end() &&
         rtnd_iter2 != pb2.children().end();
       ++rtnd_iter1, ++rtnd_iter2) {
    DFSTraverseResourceProtobufTreesReturnRTNDs(*rtnd_iter1, *rtnd_iter2,
                                                callback);
  }
  CHECK(rtnd_iter1 == pb1->mutable_children()->pointer_end());
  CHECK(rtnd_iter2 == pb2.children().end());
}

void DFSTraversePostOrderResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "DFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
       rtnd_iter = pb->mutable_children()->pointer_begin();
       rtnd_iter != pb->mutable_children()->pointer_end();
       ++rtnd_iter) {
    DFSTraversePostOrderResourceProtobufTreeReturnRTND(*rtnd_iter, callback);
  }
  callback(pb);
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
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         rtnd_iter = res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
}

void BFSTraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback) {  // NOLINT
  VLOG(3) << "BFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  queue<ResourceTopologyNodeDescriptor*> to_visit;
  to_visit.push(pb);
  while (!to_visit.empty()) {
    ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
    to_visit.pop();
    callback(res_node_desc);
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         rtnd_iter = res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
}

void BFSTraverseResourceProtobufTreeToHash(
    ResourceTopologyNodeDescriptor* pb, size_t* hash,
    boost::function<void(ResourceTopologyNodeDescriptor*, size_t*)> callback) {  // NOLINT
  VLOG(3) << "BFSTraversal of resource topology, reached "
          << pb->resource_desc().uuid()
          << ", invoking callback [" << callback << "]";
  queue<ResourceTopologyNodeDescriptor*> to_visit;
  to_visit.push(pb);
  while (!to_visit.empty()) {
    ResourceTopologyNodeDescriptor* res_node_desc = to_visit.front();
    to_visit.pop();
    callback(res_node_desc, hash);
    for (RepeatedPtrField<ResourceTopologyNodeDescriptor>::pointer_iterator
         rtnd_iter = res_node_desc->mutable_children()->pointer_begin();
         rtnd_iter != res_node_desc->mutable_children()->pointer_end();
         ++rtnd_iter) {
      to_visit.push(*rtnd_iter);
    }
  }
}

}  // namespace firmament
