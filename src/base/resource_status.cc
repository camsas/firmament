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
