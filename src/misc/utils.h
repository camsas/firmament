// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#ifndef FIRMAMENT_MISC_UTILS_H
#define FIRMAMENT_MISC_UTILS_H

#include "base/job.h"
#include "base/ensemble.h"

namespace firmament {

// Computes a UID for a job as a uint64 by hashing the job name.
// TODO(malte): This may become deprecated in the future, as the job descriptor
//              may carry this information.
uint64_t MakeJobUID(Job *job);

// Computes a UID for an ensemble as a uint64 by hashing the job name.
// TODO(malte): This may become deprecated in the future, as the ensemble
//              descriptor may carry this information.
uint64_t MakeEnsembleUID(Ensemble *ens);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_UTILS_H
