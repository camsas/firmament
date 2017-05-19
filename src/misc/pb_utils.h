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

bool DFSTraverseResourceProtobufTreeWhileTrue(
    const ResourceTopologyNodeDescriptor& pb,
    boost::function<bool(const ResourceDescriptor&)> callback);  // NOLINT

// Traversal implementation taking a callback that itself takes a
// ResourceTopologyNodeDescriptor as its argument.
void DFSTraverseResourceProtobufTreeReturnRTND(
    ResourceTopologyNodeDescriptor* pb,
    boost::function<void(ResourceTopologyNodeDescriptor*)> callback);  // NOLINT

// Traverses two resource topologies. Assumes that both topologies have the
// same structure.
void DFSTraverseResourceProtobufTreesReturnRTNDs(
    ResourceTopologyNodeDescriptor* pb1,
    const ResourceTopologyNodeDescriptor& pb2,
    boost::function<void(ResourceTopologyNodeDescriptor*,
                         const ResourceTopologyNodeDescriptor&)> callback);  // NOLINT

void DFSTraversePostOrderResourceProtobufTreeReturnRTND(
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
