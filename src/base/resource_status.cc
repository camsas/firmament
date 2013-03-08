// The Firmament project
// Copyright (c) 2011-2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Resource status representation.

#include "base/resource_status.h"

namespace firmament {

ResourceStatus::ResourceStatus(ResourceDescriptor* descr,
                               const string& endpoint_uri,
                               uint64_t last_heartbeat)
    : descriptor_(descr),
      endpoint_uri_(endpoint_uri),
      last_heartbeat_(last_heartbeat) {
}

}  // namespace firmament
