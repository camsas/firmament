// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#include <boost/functional/hash.hpp>

#include "misc/pb_utils.h"

#include "misc/equivclasses.h"

namespace firmament {

TaskEquivClass_t GenerateTaskEquivClass(const TaskDescriptor& task_desc) {
  // A level 0 TEC is the hash of the task binary name.
  size_t hash = 0;
  boost::hash_combine(hash, task_desc.binary());
  return static_cast<TaskEquivClass_t>(hash);
}

ResourceEquivClass_t GenerateResourceTopologyEquivClass(
    const ResourceTopologyNodeDescriptor& rtn_desc) {
  // A REC is the hash of the entire topology tree below this resource.
  size_t hash = 0;
  TraverseResourceProtobufTreeReturnRTND(rtn_desc,
      boost::bind(&GenerateResourceTopologyEquivClassHelper, _1, &hash));
  return static_cast<ResourceEquivClass_t>(hash);
}

void GenerateResourceTopologyEquivClassHelper(
    const ResourceTopologyNodeDescriptor& rtn_desc, size_t* hash) {
  boost::hash_combine(*hash, rtn_desc.resource_desc().type());
  // XXX(malte): the below is a hack; we shouldn't be taking the free-form name
  // here (as it confuses RECs e.g. with hostnames)!
  //boost::hash_combine(*hash, rtn_desc.resource_desc().friendly_name());
}

}  // namespace firmament
