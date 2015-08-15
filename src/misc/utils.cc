// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#include <boost/functional/hash.hpp>

// N.B.: C header for gettimeofday()
extern "C" {
#include <limits.h>
#include <openssl/sha.h>
#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
}
#include <algorithm>
#include <set>
#include <string>
#include <vector>

#ifdef __linux__
#include <sys/prctl.h>
#endif

#include "misc/utils.h"

namespace firmament {

boost::mt19937 resource_id_rg_;
boost::mt19937 job_id_rg_;
bool resource_id_rg_init_ = false;
bool job_id_rg_init_ = false;

/* Returns a timestamp in microseconds */
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

// Helper function to get the directory in which the currently
// running executable lives
int ExecutableDirectory(char *pBuf, ssize_t len) {
  char szTmp[32];
  sprintf(szTmp, "/proc/%d/exe", getpid());
  int bytes = min(readlink(szTmp, pBuf, len), len - 1);
  if(bytes >= 0)
    pBuf[bytes] = '\0';
  return bytes;
}

ResourceID_t GenerateResourceID() {
  if (!resource_id_rg_init_) {
    // In the absence of a seed, we use a crude method that captures the first
    // 100 chars of the hostname (not the FQDN).
    SetupResourceID(&resource_id_rg_, NULL);
    resource_id_rg_init_ = true;
  }
  boost::uuids::basic_random_generator<boost::mt19937> gen(&resource_id_rg_);
  return gen();
}

ResourceID_t GenerateResourceID(const string& seed) {
  if (!resource_id_rg_init_) {
    // Use the seed string to create a hash with which we seed the RNG.
    SetupResourceID(&resource_id_rg_, seed.c_str());
    resource_id_rg_init_ = true;
  }
  boost::uuids::basic_random_generator<boost::mt19937> gen(&resource_id_rg_);
  return gen();
}

void SetupResourceID(boost::mt19937 *resource_id, const char *seed) {
  size_t hash = 42;
  char hn[100];
  bzero(&hn, 100);
  if (seed == NULL) {
    gethostname(hn, 100);
  } else {
    snprintf(hn, sizeof(hn), "%s", seed);
  }
  // Hash the hostname (truncated to 100 characters)
  boost::hash_combine(hash, hn);
  VLOG(2) << "Seeding resource ID RNG with " << hash << " from seed "
          << hn;
  resource_id->seed(hash);
}

ResourceID_t GenerateRootResourceID(const string& hostname) {
  size_t hash = 42;
  // Hash the hostname
  boost::hash_combine(hash, hostname);
  VLOG(2) << "Seeing resource ID RNG with " << hash << " from hostname "
          << hostname;
  resource_id_rg_.seed(hash);
  resource_id_rg_init_ = true;
  boost::uuids::basic_random_generator<boost::mt19937> gen(&resource_id_rg_);
  return gen();
}

JobID_t GenerateJobID() {
  if (!job_id_rg_init_) {
    job_id_rg_.seed(time(NULL));
    job_id_rg_init_ = true;
  }
  boost::uuids::basic_random_generator<boost::mt19937> gen(&job_id_rg_);
  return gen();
}

JobID_t GenerateJobID(uint64_t job_id) {
  size_t hash = 42;
  // Hash the hostname
  boost::hash_combine(hash, job_id);
  job_id_rg_.seed(hash);
  job_id_rg_init_ = true;
  boost::uuids::basic_random_generator<boost::mt19937> gen(&job_id_rg_);
  return gen();
}

TaskID_t GenerateRootTaskID(const JobDescriptor& job_desc) {
  size_t hash = 42;
  //boost::hash_combine(hash, job_desc.uuid());
  boost::hash_combine(hash, job_desc.name());
  boost::hash_combine(hash, job_desc.root_task().binary());

  return static_cast<TaskID_t>(hash);
}

TaskID_t GenerateTaskID(const TaskDescriptor& parent_task) {
  // A new task's ID is a hash of the parent (spawning) task's ID and its
  // current spawn counter value, which is implicitly stored in the TD by means
  // of the length of its set of spawned tasks.
  size_t hash = 42;
  boost::hash_combine(hash, parent_task.uid());
  boost::hash_combine(hash, parent_task.spawned_size());
  return static_cast<TaskID_t>(hash);
}

TaskID_t GenerateTaskID(const TaskDescriptor& parent_task, uint64_t child_num) {
  // A new task's ID is a hash of the parent (spawning) task's ID and its
  // current spawn counter value, which is implicitly stored in the TD by means
  // of the length of its set of spawned tasks.
  size_t hash = 42;
  boost::hash_combine(hash, parent_task.uid());
  boost::hash_combine(hash, child_num);
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
  uint8_t hash[SHA256_DIGEST_LENGTH];
  SHA256_CTX ctx;
  SHA256_Init(&ctx);
  SHA256_Update(&ctx, &producing_task, sizeof(TaskID_t));
  SHA256_Update(&ctx, &output_id, sizeof(TaskOutputID_t));
  SHA256_Final(hash, &ctx);
  DataObjectID_t doid(hash);
  return doid;
}

size_t HashCommandLine(const TaskDescriptor& td) {
  size_t hash = 42;
  boost::hash_combine(hash, td.binary());
  for (auto it = td.args().begin(); it != td.args().end(); ++it) {
    boost::hash_combine(hash, *it);
  }
  return hash;
}

size_t HashJobID(JobID_t job_id) {
  size_t hash = 42;
  boost::hash_combine(hash, job_id);
  return hash;
}

size_t HashJobID(const TaskDescriptor& td) {
  size_t hash = 42;
  boost::hash_combine(hash, td.job_id());
  return hash;
}

size_t HashString(const string& str) {
  size_t hash = 42;
  boost::hash_combine(hash, str);
  return hash;
}

DataObjectID_t DataObjectIDFromString(const string& str) {
  // N.B.: This assumes that the string is a readable, hexadecimal
  // representation of the ID.
  DataObjectID_t object_id(str, true);
  return object_id;
}

bool IsEqual(double first, double second) {
  if (fabs(first - second) < COMPARE_EPS) {
    return true;
  }
  return false;
}

DataObjectID_t DataObjectIDFromProtobuf(const string& str) {
  // N.B.: This assumes that the string is a binary representation of the ID.
  DataObjectID_t object_id(str, false);
  return object_id;
}

DataObjectID_t DataObjectIDFromProtobuf(const ReferenceDescriptor& rd) {
  // N.B.: This assumes that the string is a binary representation of the ID.
  DataObjectID_t object_id(rd.id(), false);
  return object_id;
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
  stringstream strm(str);
  TaskID_t task_uuid;
  strm >> task_uuid;
  return task_uuid;
}

// Pipe setup
// errfd[0] == PARENT_READ
// errfd[1] == CHILD_WRITE
// outfd[0] == PARENT_READ
// outfd[1] == CHILD_WRITE
// infd[0] == CHILD_READ
// infd[1] == PARENT_WRITE

int32_t ExecCommandSync(const string& cmdline, vector<string> args,
                        int infd[2], int outfd[2], int errfd[2]) {
  VLOG(2) << "Executing externally: " << cmdline;
  pid_t pid;
  if (pipe(infd) != 0) {
    LOG(ERROR) << "Failed to create pipe to task.";
  }
  if (pipe(outfd) != 0) {
    LOG(ERROR) << "Failed to create pipe from task.";
  }
  if (pipe(errfd) != 0) {
      LOG(ERROR) << "Failed to create pipe from task.";
  }
  // Convert args from string to char*
  vector<char*> argv;
  // no performance monitoring, so we only need to reserve space for the
  // default and NULL args
  argv.reserve(args.size() + 1);
  argv.push_back((char*)(cmdline.c_str()));  // NOLINT
  for (uint32_t i = 0; i < args.size(); ++i) {
    // N.B.: This casts away the const qualifier on the c_str() result.
    // This is joyfully unsafe, of course.
    argv.push_back((char*)(args[i].c_str()));  // NOLINT
  }
  // The last argument to execvp is always NULL.
  argv.push_back(NULL);
  // Print the whole command line
  string full_cmd_line;
  for (vector<char*>::const_iterator arg_iter = argv.begin();
       arg_iter != argv.end();
       ++arg_iter) {
    if (*arg_iter != NULL) {
      full_cmd_line += *arg_iter;
      full_cmd_line += " ";
    }
  }
  LOG(INFO) << "External execution of command: " << full_cmd_line;
  pid = fork();
  switch (pid) {
    case -1:
      // Error
      PLOG(ERROR) << "Failed to fork child process.";
      break;
    case 0: {
      // Child
      int fd;
      int fds;

      // set up pipes
      CHECK(dup2(infd[0], STDIN_FILENO) == STDIN_FILENO);
      CHECK(dup2(outfd[1], STDOUT_FILENO) == STDOUT_FILENO);
      CHECK(dup2(errfd[1], STDERR_FILENO) == STDERR_FILENO);

      // XXX(ionel): It's not clear to me while we're closing all the fds >= 3.

      // Close all file descriptors other than stdin, stdout and stderr
      if ((fds = getdtablesize()) == -1) fds = OPEN_MAX_GUESS;
      for (fd = 3; fd < fds; fd++) close(fd);

      // kill child process if parent terminates
      // SOMEDAY(adam): make this portable beyond Linux?
#ifdef __linux__
      prctl(PR_SET_PDEATHSIG, SIGHUP);
#endif

      // Run the task binary
      execvp(argv[0], &argv[0]);
      // execl only returns if there was an error
      PLOG(ERROR) << "execvp failed for task command '" << full_cmd_line << "'";
      //ReportTaskExecutionFailure();
      _exit(1);
      break;
    }
    default:
      // Parent
      VLOG(1) << "Subprocess with PID " << pid << " created.";
      // close unused pipe ends
      close(infd[0]);
      close(outfd[1]);
      close(errfd[1]);
      // TODO(malte): ReadFromPipe is a synchronous call that will only return
      // once the pipe has been closed! Check if this is actually the semantic
      // we want.
      // The fact that we cannot concurrently read from the STDOUT and the
      // STDERR pipe this way suggest the answer is that it is not...
      //ReadFromPipe(pipe_from[0]);
      //ReadFromPipe(pipe_from[1]);
      return pid;
  }
  return -1;
}

int32_t WaitForFinish(pid_t pid) {
  // Wait for task to terminate
  int status;
  while (waitpid(pid, &status, 0) != pid) {
    VLOG(2) << "Waiting for child process " << pid << " to exit...";
  }
  if (WIFEXITED(status)) {
    VLOG(1) << "Subprocess with PID " << pid << " exited with status "
            << WEXITSTATUS(status);
  } else if (WIFSIGNALED(status)) {
    VLOG(1) << "Subprocess with PID " << pid << " exited due to uncaught "
            << "signal " << WTERMSIG(status);
  } else if (WIFSTOPPED(status)) {
    VLOG(1) << "Subprocess with PID " << pid << " is stopped due to "
            << "signal " << WSTOPSIG(status);
  } else {
    LOG(ERROR) << "Unexpected exit status: " << hex << status;
  }
  return status;
}

uint8_t* SHA256Hash(uint8_t* bytes, uint64_t len) {
  uint8_t* hash = new uint8_t[SHA256_DIGEST_LENGTH];
  SHA256_CTX ctx;
  SHA256_Init(&ctx);
  SHA256_Update(&ctx, bytes, len);
  SHA256_Final(hash, &ctx);
  return hash;
}

// Helper function to convert a repeated bytes field to a set of
// DataObjectID_t.
// This method copies the input collection, so it is O(N) in time and space.
set<DataObjectID_t*> DataObjectIDsFromProtobuf(
    const RepeatedPtrField<string>& pb_field) {
  set<DataObjectID_t*> return_set;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_set.insert(new DataObjectID_t(*iter, false));
  return return_set;
}

// Helper function to convert a repeated bytes field to a set of
// DataObjectID_t.
// This method copies the input collection, so it is O(N) in time and space.
set<DataObjectID_t*> DataObjectIDsFromProtobuf(
    const RepeatedPtrField<ReferenceDescriptor>& pb_field) {
  set<DataObjectID_t*> return_set;
  // N.B.: using GNU-style RTTI (typeof)
  for (__typeof__(pb_field.begin()) iter = pb_field.begin();
       iter != pb_field.end();
       ++iter)
    return_set.insert(new DataObjectID_t(iter->id(), false));
  return return_set;
}

// Helper function to convert second-granularity timestamps
// to strings
string CoarseTimestampToHumanReadble(const time_t rawtime) {
  struct tm * dt;
  char buffer[30];
  dt = localtime(&rawtime);
  strftime(buffer, sizeof(buffer), "%Y%m%d:%H:%M", dt);
  return string(buffer);
}

ResourceID_t PickRandomResourceID(
    const unordered_set<ResourceID_t,
      boost::hash<boost::uuids::uuid>>& leaf_res_ids) {
  // An unordered set does not have a random access iterator. Hence,
  // a solution that simply picks a random index and moves an iterator to that
  // index has O(N) complexity. In order to get a random element faster the
  // code first picks a random non-empty bucket. Following, it randomly
  // selects a value in that bucket. The complexity of this solution is
  // O(M), where M is the number of collisions in the bucket.
  uint32_t rand_seed = 0;
  size_t bucket_index = 0;
  size_t bucket_size = 0;
  while (bucket_size == 0) {
    bucket_index = rand_r(&rand_seed) % leaf_res_ids.bucket_count();
    bucket_size = leaf_res_ids.bucket_size(bucket_index);
  }
  size_t index_within_bucket = rand_r(&rand_seed) % bucket_size;
  auto it = leaf_res_ids.begin(bucket_index);
  advance(it, index_within_bucket);
  return *it;
}

}  // namespace firmament
