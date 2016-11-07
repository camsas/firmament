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

// Miscellaneous utility functions. Descriptions with their declarations.

#ifndef FIRMAMENT_MISC_UTILS_H
#define FIRMAMENT_MISC_UTILS_H

#include <set>
#include <string>
#include <vector>

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#endif

#include <google/protobuf/descriptor.h>

#include "base/common.h"
#include "base/types.h"
#include "boost/lexical_cast.hpp"

namespace firmament {

using google::protobuf::EnumDescriptor;

#define COMPARE_EPS 0.00000000001
#define SEED 42

#define ENUM_TO_STRING(t, v) t ## _descriptor()->FindValueByNumber(v)->name()

#ifdef OPEN_MAX
#define OPEN_MAX_GUESS OPEN_MAX
#else
#define OPEN_MAX_GUESS 256 // reasonable value
#endif

// Computes a UID for a job as a uint64 by hashing the job name.
// TODO(malte): This may become deprecated in the future, as the job descriptor
//              may carry this information.
// uint64_t MakeJobUID(Job *job);

int ExecutableDirectory(char *pBuf, ssize_t len);
ResourceID_t GenerateResourceID();
ResourceID_t GenerateResourceID(const string& seed);
ResourceID_t GenerateRootResourceID(const string& hostname);
JobID_t GenerateJobID();
JobID_t GenerateJobID(uint64_t job_id);
DataObjectID_t GenerateDataObjectID(const TaskDescriptor& task_descriptor);
DataObjectID_t GenerateDataObjectID(TaskID_t producing_task,
                                    TaskOutputID_t output_id);
// XXX(malte): This is a hack. Figure out a proper hashing function compatible
// with root tasks.
TaskID_t GenerateRootTaskID(const JobDescriptor& job_desc);
TaskID_t GenerateTaskID(const TaskDescriptor& parent_task);
TaskID_t GenerateTaskID(const TaskDescriptor& parent_task, uint64_t child_num);
uint64_t HashCommandLine(const TaskDescriptor& td);
uint64_t HashInt(const uint64_t input);
uint64_t HashJobID(const TaskDescriptor& td);
uint64_t HashString(const string& str);
bool IsEqual(double first, double second);
void MkdirIfNotPresent(const string &directory);
// Utility functions to parse various types from strings.
DataObjectID_t DataObjectIDFromString(const string& str);
DataObjectID_t DataObjectIDFromProtobuf(const string& str);
ResourceID_t MachineResIDForResource(shared_ptr<ResourceMap_t> resource_map,
                                     ResourceID_t res_id);
ResourceID_t ResourceIDFromString(const string& str);
JobID_t JobIDFromString(const string& str);
uint64_t UpdateTaskTotalRunTime(const TaskDescriptor& td);
uint64_t UpdateTaskTotalUnscheduledTime(const TaskDescriptor& td);
void SetupResourceID(boost::mt19937 *resource_id, const char *hostname);
TaskID_t TaskIDFromString(const string& str);

int32_t ExecCommandSync(const string& cmdline, vector<string> args,
                        int infd[2], int outfd[2], int errfd[2]);
int32_t WaitForFinish(pid_t pid);

uint8_t* SHA256Hash(uint8_t* bytes, uint64_t len);

string CoarseTimestampToHumanReadble(const time_t rawtime);

unordered_set<DataObjectID_t*> DataObjectIDsFromProtobuf(
    const RepeatedPtrField<string>& pb_field);
unordered_set<DataObjectID_t*> DataObjectIDsFromProtobuf(
    const RepeatedPtrField<ReferenceDescriptor>& pb_field);

/**
 * Pick a random value from an unordered set of resource ids.
 * @return the randomly selected resource id
 */
ResourceID_t PickRandomResourceID(
    const unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>& leaf_res_ids);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_UTILS_H
