// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
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

//#include "base/job.h"
//#include "base/ensemble.h"
#include "base/common.h"
#include "base/types.h"
#include "boost/lexical_cast.hpp"

namespace firmament {

#define ENUM_TO_STRING(t, v) t ## _descriptor()->FindValueByNumber(v)->name()

// Returns the current epoch timestamp in µ-seconds as an integer.
// Uses gettimeofday() under the hood, so does not make any guarantees w.r.t.
// time zones etc.
uint64_t GetCurrentTimestamp();

// Computes a UID for a job as a uint64 by hashing the job name.
// TODO(malte): This may become deprecated in the future, as the job descriptor
//              may carry this information.
/*uint64_t MakeJobUID(Job *job);

// Computes a UID for an ensemble as a uint64 by hashing the job name.
// TODO(malte): This may become deprecated in the future, as the ensemble
//              descriptor may carry this information.
uint64_t MakeEnsembleUID(Ensemble *ens);*/

ResourceID_t GenerateUUID();
JobID_t GenerateJobID();
DataObjectID_t GenerateDataObjectID(const TaskDescriptor& task_descriptor);
DataObjectID_t GenerateDataObjectID(TaskID_t producing_task,
                                    TaskOutputID_t output_id);
// XXX(malte): This is a hack. Figure out a proper hashing function compatible
// with root tasks.
TaskID_t GenerateRootTaskID(const JobDescriptor& job_desc);
TaskID_t GenerateTaskID(const TaskDescriptor& parent_task);
// Utility functions to parse various types from strings.
DataObjectID_t DataObjectIDFromString(const string& str);
DataObjectID_t DataObjectIDFromProtobuf(const string& str);
ResourceID_t ResourceIDFromString(const string& str);
JobID_t JobIDFromString(const string& str);
TaskID_t TaskIDFromString(const string& str);

int32_t ExecCommandSync(const string& cmdline, vector<string> args,
                        int infd[2], int outfd[2]);

uint8_t* SHA256Hash(uint8_t* bytes, uint64_t len);

set<DataObjectID_t*> DataObjectIDsFromProtobuf(
    const RepeatedPtrField<string>& pb_field);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_UTILS_H
