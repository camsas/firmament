// The Firmament project
// Copyright (c) 2011-2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Resource status representation.

#ifndef FIRMAMENT_BASE_RESOURCE_STATUS_H
#define FIRMAMENT_BASE_RESOURCE_STATUS_H

#include <string>

#include "base/common.h"
#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"

namespace firmament {

class ResourceStatus {
 public:
  ResourceStatus(ResourceDescriptor* descr,
                 ResourceTopologyNodeDescriptor* rtnd,
                 const string& endpoint_uri,
                 uint64_t last_heartbeat);
  inline ResourceDescriptor* mutable_descriptor() { return descriptor_; }
  inline const ResourceDescriptor& descriptor() { return *descriptor_; }
  inline const string& location() { return endpoint_uri_; }
  inline uint64_t last_heartbeat() { return last_heartbeat_; }
  inline void set_last_heartbeat(uint64_t hb) { last_heartbeat_ = hb; }
  inline ResourceTopologyNodeDescriptor* mutable_topology_node() {
    return topology_node_;
  }
  inline const ResourceTopologyNodeDescriptor& topology_node() {
    return *topology_node_;
  }
 protected:
  ResourceDescriptor* descriptor_;
  ResourceTopologyNodeDescriptor* topology_node_;
  string endpoint_uri_;
  uint64_t last_heartbeat_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_RESOURCE_STATUS_H
