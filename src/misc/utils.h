// TODO: header

#ifndef FIRMAMENT_MISC_UTILS_H
#define FIRMAMENT_MISC_UTILS_H

#include "base/job.h"
#include "base/ensemble.h"

namespace firmament {

uint64_t MakeJobUID(Job *job);
uint64_t MakeEnsembleUID(Ensemble *ens);

}  // namespace firmament

#endif  // FIRMAMENT_MISC_UTILS_H
