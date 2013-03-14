// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Miscellaneous utility functions. Descriptions with their declarations.

#include <boost/functional/hash.hpp>

// N.B.: C header for gettimeofday()
extern "C" {
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <sys/time.h>
}
#include <string>
#include <vector>

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

// Pipe setup
// outfd[0] == PARENT_READ
// outfd[1] == CHILD_WRITE
// infd[0] == CHILD_READ
// infd[1] == PARENT_WRITE
int32_t ExecCommandSync(const string& cmdline, vector<string> args,
                        int infd[2], int outfd[2]) {
  pid_t pid;
  if (pipe(infd) != 0) {
    LOG(ERROR) << "Failed to create pipe to task.";
  }
  if (pipe(outfd) != 0) {
    LOG(ERROR) << "Failed to create pipe from task.";
  }
  pid = fork();
  switch (pid) {
    case -1:
      // Error
      LOG(ERROR) << "Failed to fork child process.";
      break;
    case 0: {
      // Child
      // Close parent pipe descriptors
      close(infd[1]);
      close(outfd[0]);
      // set up pipes
      CHECK(dup2(infd[0], STDIN_FILENO) == STDIN_FILENO);
      CHECK(dup2(outfd[1], STDOUT_FILENO) == STDOUT_FILENO);
      // close unnecessary pipe descriptors
      close(infd[0]);
      close(outfd[1]);
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
      // Run the task binary
      execvp(argv[0], &argv[0]);
      // execl only returns if there was an error
      PLOG(ERROR) << "execvp failed for task command '" << full_cmd_line << "'";
      //ReportTaskExecutionFailure();
      _exit(1);
    }
    default:
      // close unused pipe ends
      close(infd[0]);
      close(outfd[1]);
      // TODO(malte): ReadFromPipe is a synchronous call that will only return
      // once the pipe has been closed! Check if this is actually the semantic
      // we want.
      // The fact that we cannot concurrently read from the STDOUT and the
      // STDERR pipe this way suggest the answer is that it is not...
      //ReadFromPipe(pipe_from[0]);
      //ReadFromPipe(pipe_from[1]);
      // wait for task to terminate
      int status;
      while (!WIFEXITED(status)) {
        // VLOG_EVERY_N(2, 1000) << "Waiting for task to exit...";
        waitpid(pid, &status, 0);
      }
      VLOG(1) << "Task process with PID " << pid << " exited with status "
              << WEXITSTATUS(status);
      return status;
  }
  return -1;
}

}  // namespace firmament
