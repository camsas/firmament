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
    const ResourceTopologyNodeDescriptor* pb,
    boost::function<void(const ResourceTopologyNodeDescriptor*)> callback);  // NOLINT

}  // namespace firmament

#endif  // FIRMAMENT_MISC_PB_UTILS_H
