// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#ifndef FIRMAMENT_MISC_UTILS_H
#define FIRMAMENT_MISC_UTILS_H

#ifdef __PLATFORM_HAS_BOOST__
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#endif

//#include "base/job.h"
//#include "base/ensemble.h"
#include "base/common.h"
#include "base/types.h"

namespace firmament {

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
JobID_t JobIDFromString(const string& str);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_UTILS_H
