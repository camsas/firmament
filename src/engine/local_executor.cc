// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Local executor class.

#include "engine/local_executor.h"

extern "C" {
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
}

#include "base/common.h"
#include "base/types.h"

namespace firmament {
namespace executor {

using common::pb_to_vector;

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      topology_manager_(shared_ptr<TopologyManager>()) {  // NULL
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "No topology manager passed, so will not bind to resource.";
}

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri,
                             shared_ptr<TopologyManager> topology_mgr)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      topology_manager_(topology_mgr) {
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "Tasks will be bound to the resource by the topology manager"
          << "at " << topology_manager_;
}

void LocalExecutor::RunTask(TaskDescriptor* td,
                            bool firmament_binary) {
  CHECK(td);
  // XXX(malte): Move this over to use RunProcessAsync, instead of custom thread
  // spawning.
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  boost::thread per_task_thread(
      boost::bind(&LocalExecutor::_RunTask, this, td, firmament_binary));
}

bool LocalExecutor::_RunTask(TaskDescriptor* td,
                             bool firmament_binary) {
  SetUpEnvironmentForTask(*td);
  // Convert arguments as specified in TD into a string vector that we can munge
  // into an actual argv[].
  vector<string> args = pb_to_vector(td->args());
  // TODO(malte): This is somewhat hackish
  // arguments: binary (path + name), arguments, performance monitoring on/off,
  // is this a Firmament task binary? (on/off; will cause default arugments to
  // be passed)
  bool res = (RunProcessSync(td->binary(), args, false, firmament_binary) == 0);
  return res;
}

int32_t LocalExecutor::RunProcessAsync(const string& cmdline,
                                       vector<string> args,
                                       bool perf_monitoring,
                                       bool default_args) {
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  boost::thread async_process_thread(
      boost::bind(&LocalExecutor::RunProcessSync, this, cmdline, args,
                  perf_monitoring, default_args));
  // We hard-code the return to zero here; maybe should return a thread
  // reference instead.
  return 0;
}

int32_t LocalExecutor::RunProcessSync(const string& cmdline,
                                      vector<string> args,
                                      bool perf_monitoring,
                                      bool default_args) {
  pid_t pid;
  int pipe_to[2];    // pipe to feed input data to task
  int pipe_from[3];  // pipe to receive output data from task
  if (pipe(pipe_to) != 0) {
    LOG(ERROR) << "Failed to create pipe to task.";
  }
  if (pipe(pipe_from) != 0) {
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
      // set up pipes
      dup2(pipe_to[0], STDIN_FILENO);
      dup2(pipe_from[1], STDOUT_FILENO);
      //dup2(pipe_from[2], STDERR_FILENO);
      // close unnecessary pipe descriptors
      close(pipe_to[0]);
      close(pipe_to[1]);
      close(pipe_from[0]);
      close(pipe_from[1]);
      //close(pipe_from[2]);
      // Convert args from string to char*
      vector<char*> argv;
      // argv[0] is always the command name
      if (perf_monitoring) {
        // performance monitoring is active, so reserve extra space for the
        // "perf" invocation prefix.
        argv.reserve(args.size() + (default_args ? 11 : 10));
        argv.push_back((char*)("perf"));  // NOLINT
        argv.push_back((char*)("stat"));  // NOLINT
        argv.push_back((char*)("-x"));  // NOLINT
        argv.push_back((char*)(","));  // NOLINT
        argv.push_back((char*)("-o"));  // NOLINT
        argv.push_back((char*)(getenv("PERF_FNAME")));  // NOLINT
        argv.push_back((char*)("-e"));  // NOLINT
        argv.push_back((char*)("instructions,cache-misses"));  // NOLINT
        argv.push_back((char*)("--"));  // NOLINT
      } else {
        // no performance monitoring, so we only need to reserve space for the
        // default and NULL args
        argv.reserve(args.size() + (default_args ? 2 : 1));
      }
      argv.push_back((char*)(cmdline.c_str()));  // NOLINT
      if (default_args)
        argv.push_back(
            (char*)"--tryfromenv=coordinator_uri,resource_id,task_id");  // NOLINT
      for (uint32_t i = 0; i < args.size(); ++i) {
        // N.B.: This casts away the const qualifier on the c_str() result.
        // This is joyfully unsafe, of course.
        VLOG(1) << "Adding extra argument \"" << args[i] << "\"";
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
      LOG(INFO) << "COMMAND LINE: " << full_cmd_line;
      // Run the task binary
      execvp(argv[0], &argv[0]);
      // execl only returns if there was an error
      PLOG(ERROR) << "execvp failed for task command '" << full_cmd_line << "'";
      //ReportTaskExecutionFailure();
      _exit(1);
    }
    default:
      // Parent
      VLOG(1) << "Task process with PID " << pid << " created.";
      // Pin the task to the appropriate resource
      if (topology_manager_)
        topology_manager_->BindPIDToResource(pid, local_resource_id_);
      // close unused pipe ends
      close(pipe_to[0]);
      close(pipe_from[1]);
      // TODO(malte): fix the pipe stuff to work properly
      close(pipe_to[1]);
      // TODO(malte): ReadFromPipe is a synchronous call that will only return
      // once the pipe has been closed! Check if this is actually the semantic
      // we want.
      // The fact that we cannot concurrently read from the STDOUT and the
      // STDERR pipe this way suggest the answer is that it is not...
      ReadFromPipe(pipe_from[0]);
      //ReadFromPipe(pipe_from[1]);
      // wait for task to terminate
      int status;
      while (!WIFEXITED(status)) {
        VLOG_EVERY_N(2, 1000) << "Waiting for task to exit...";
        waitpid(pid, &status, 0);
      }
      VLOG(1) << "Task process with PID " << pid << " exited with status "
              << WEXITSTATUS(status);
      return status;
  }
  return -1;
}

string LocalExecutor::PerfDataFileName(const TaskDescriptor& td) {
  return (to_string(local_resource_id_) + "-" + to_string(td.uid()) + ".perf");
}

void LocalExecutor::SetUpEnvironmentForTask(const TaskDescriptor& td) {
  if (coordinator_uri_.empty())
    LOG(WARNING) << "Executor does not have the coordinator_uri_ field set. "
                 << "The task will be unable to communicate with the "
                 << "coordinator!";
  vector<EnvPair_t> env;
  env.push_back(EnvPair_t("FLAGS_task_id", to_string(td.uid())));
  env.push_back(EnvPair_t("PERF_FNAME", PerfDataFileName(td)));
  env.push_back(EnvPair_t("FLAGS_coordinator_uri", coordinator_uri_));
  env.push_back(EnvPair_t("FLAGS_resource_id", to_string(local_resource_id_)));
  // Set environment variables
  VLOG(2) << "Task's environment variables:";
  for (vector<EnvPair_t>::const_iterator env_iter = env.begin();
       env_iter != env.end();
       ++env_iter) {
    VLOG(2) << "  " << env_iter->first << ": " << env_iter->second;
    setenv(env_iter->first.c_str(), env_iter->second.c_str(), 1);
  }
}

void LocalExecutor::WriteToPipe(int fd, void* data, size_t len) {
  FILE *stream;
  // Open the pipe
  if ((stream = fdopen(fd, "w")) == NULL) {
    LOG(ERROR) << "Failed to open pipe for writing. FD: " << fd;
  }
  // Write the data to the pipe
  fwrite(data, len, 1, stream);
  // Finally, close the pipe
  fclose(stream);
}

void LocalExecutor::ReadFromPipe(int fd) {
  FILE *stream;
  int ch;
  if ((stream =fdopen(fd, "r")) == NULL) {
    LOG(ERROR) << "Failed to open pipe for reading. FD " << fd;
  }
  while ( (ch = getc(stream)) != EOF ) {
    // XXX(malte): temp hack
    putc(ch, stdout);
  }
  fflush(stdout);
  fclose(stream);
}

}  // namespace executor
}  // namespace firmament
