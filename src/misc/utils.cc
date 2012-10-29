// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#include <boost/functional/hash.hpp>

#include <string>
// N.B.: C header for gettimeofday()
#include <sys/time.h>

#include "misc/utils.h"

namespace firmament {

uint64_t GetCurrentTimestamp() {
  struct timeval ts;
  gettimeofday(&ts, NULL);
  return ts.tv_sec * 1000000 + ts.tv_usec;
}

/*uint64_t MakeJobUID(Job *job) {
  CHECK_NOTNULL(job);
  boost::hash<string> hasher;
  return hasher(job->name());
}

uint64_t MakeEnsembleUID(Ensemble *ens) {
  CHECK_NOTNULL(ens);
  VLOG(1) << ens->name();
  boost::hash<string> hasher;
  return hasher(ens->name());
}*/

ResourceID_t GenerateUUID() {
  boost::uuids::random_generator gen;
  return gen();
}

JobID_t GenerateJobID() {
  boost::uuids::random_generator gen;
  return gen();
}

JobID_t JobIDFromString(const string& str) {
  // XXX(malte): This makes assumptions about JobID_t being a Boost UUID. We
  // should have a generic "JobID_t-from-string" helper instead.
#ifdef __PLATFORM_HAS_BOOST__
  boost::uuids::string_generator gen;
  boost::uuids::uuid job_uuid = gen(str);
#else
  string job_uuid = str;
#endif
  return job_uuid;
}

}  // namespace firmament
