// The Firmament project
// Copyright (c) 2011-2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Resource status representation.

#include "base/resource_desc.pb.h"
#include "base/resource_topology_node_desc.pb.h"

#include "base/resource_status.h"

namespace firmament {

ResourceStatus::ResourceStatus(ResourceDescriptor* descr,
                               ResourceTopologyNodeDescriptor* rtnd,
                               const string& endpoint_uri,
                               uint64_t last_heartbeat)
    : descriptor_(descr),
      topology_node_(rtnd),
      endpoint_uri_(endpoint_uri),
      last_heartbeat_(last_heartbeat) {
}

}  // namespace firmament
