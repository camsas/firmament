// The Firmament project
// Copyright (c) 2013 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Equivalence class generation and maintenance code.

#ifndef FIRMAMENT_MISC_EQUIVCLASSES_H
#define FIRMAMENT_MISC_EQUIVCLASSES_H

#include "base/common.h"
#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"

namespace firmament {

typedef uint64_t ResourceEquivClass_t;
typedef uint64_t TaskEquivClass_t;

TaskEquivClass_t GenerateTaskEquivClass(const TaskDescriptor& task_desc);
ResourceEquivClass_t GenerateResourceTopologyEquivClass(
    const ResourceTopologyNodeDescriptor& rtn_desc);

//private
void GenerateResourceTopologyEquivClassHelper(
    const ResourceTopologyNodeDescriptor& rtn_desc, size_t* hash);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_EQUIVCLASSES_H
