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

boost::mt19937 ran_;
bool ran_init_ = false;

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
  if (!ran_init_) {
    ran_.seed(time(NULL));
    ran_init_ = true;
  }
  boost::uuids::basic_random_generator<boost::mt19937> gen(&ran_);
  return gen();
}

JobID_t GenerateJobID() {
  if (!ran_init_) {
    ran_.seed(time(NULL));
    ran_init_ = true;
  }
  boost::uuids::basic_random_generator<boost::mt19937> gen(&ran_);
  return gen();
}

TaskID_t GenerateRootTaskID(const JobDescriptor& job_desc) {
  size_t hash = 0;
  boost::hash_combine(hash, job_desc.uuid());
  return static_cast<TaskID_t>(hash);
}

TaskID_t GenerateTaskID(const TaskDescriptor& parent_task) {
  // A new task's ID is a hash of the parent (spawning) task's ID and its
  // current spawn counter value, which is implicitly stored in the TD by means
  // of the length of its set of spawned tasks.
  size_t hash = 0;
  boost::hash_combine(hash, parent_task.uid());
  boost::hash_combine(hash, parent_task.spawned_size());
  return static_cast<TaskID_t>(hash);
}

DataObjectID_t GenerateDataObjectID(const TaskDescriptor& producing_task) {
  // A thin shim that converts to the signature of GenerateDataObjectID.
  return GenerateDataObjectID(producing_task.uid(),
                              producing_task.outputs_size());
}

DataObjectID_t GenerateDataObjectID(
    TaskID_t producing_task, TaskOutputID_t output_id) {
  // A new data object ID is allocated by hashing the ID of the producing task
  // and the ID of the output (which may be greater than the number of declared
  // output IDs, since tasks can produce extra outputs).
  // TODO(malte): This is not *quite* the same as CIEL's naming scheme (which is
  // a little cleverer and uses the task argument structure here), but works for
  // now. Revisit later.
  size_t hash = 0;
  boost::hash_combine(hash, producing_task);
  boost::hash_combine(hash, output_id);
  return static_cast<DataObjectID_t>(hash);
}

DataObjectID_t DataObjectIDFromString(const string& str) {
  // XXX(malte): possibly unsafe use of atol() here.
  DataObjectID_t object_id = atol(str.c_str());
  return object_id;
}

const char* StringFromDataObjectId(DataObjectID_t id) {
  // XXX(malte): possibly unsafe use of atol() here.
  string str= boost::lexical_cast<string>(id);
  return str.c_str(); 
}
const char* StringFromDataObjectIdMut(DataObjectID_t id) {
  // XXX(malte): possibly unsafe use of atol() here.
  string str= boost::lexical_cast<string>(id) + "mut";
  return str.c_str(); 
}

const char* StringFromDataObjectIdObj(DataObjectID_t id) {
  // XXX(malte): possibly unsafe use of atol() here.
  string str= boost::lexical_cast<string>(id) + "obj";
  return str.c_str(); 
}

const char* StringFromDataObjectIdSize(DataObjectID_t id) {
  // XXX(malte): possibly unsafe use of atol() here.
  string str= boost::lexical_cast<string>(id) + "size";
  return str.c_str(); 
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

ResourceID_t ResourceIDFromString(const string& str) {
  // XXX(malte): This makes assumptions about ResourceID_t being a Boost UUID.
  // We should have a generic "JobID_t-from-string" helper instead.
#ifdef __PLATFORM_HAS_BOOST__
  boost::uuids::string_generator gen;
  boost::uuids::uuid res_uuid = gen(str);
#else
  string res_uuid = str;
#endif
  return res_uuid;
}

TaskID_t TaskIDFromString(const string& str) {
  // XXX(malte): possibly unsafe use of atol() here.
  TaskID_t task_uuid = strtoul(str.c_str(), NULL, 10);
  return task_uuid;
}

}  // namespace firmament
