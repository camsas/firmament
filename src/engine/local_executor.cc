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
#include <boost/regex.hpp>

#include "base/common.h"
#include "base/types.h"
#include "misc/equivclasses.h"

DEFINE_bool(debug_tasks, false,
            "Run tasks through a debugger (gdb).");
DEFINE_uint64(debug_interactively, 0,
              "Run this task ID inside an interactive debugger.");
DEFINE_bool(perf_monitoring, true,
            "Enable performance monitoring for tasks executed.");

namespace firmament {
namespace executor {

using common::pb_to_vector;

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      topology_manager_(shared_ptr<TopologyManager>()),  // NULL
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "No topology manager passed, so will not bind to resource.";
}

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri,
                             shared_ptr<TopologyManager> topology_mgr)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      topology_manager_(topology_mgr),
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "Tasks will be bound to the resource by the topology manager"
          << "at " << topology_manager_;
}

char* LocalExecutor::AddPerfMonitoringToCommandLine(vector<char*>* argv) {
  // Define the string prefix for performance monitoring
  //string perf_string = "perf stat -x, -o ";
  VLOG(2) << "Enabling performance monitoring...";
  string perf_string = "perf stat -o ";
  perf_string += getenv("PERF_FNAME");
  perf_string += " -e instructions,cycles,cache-misses,cache-references -- ";
  return TokenizeIntoArgv(perf_string, argv);
}

char* LocalExecutor::AddDebuggingToCommandLine(vector<char*>* argv) {
  // Define the string prefix for debugging
  string dbg_string;
  VLOG(2) << "Enabling debugging...";
  if (FLAGS_debug_tasks)
    dbg_string = "gdb -batch -ex run --args ";
  else if (FLAGS_debug_interactively != 0)
    dbg_string = "gdb '-ex run --args ";
  return TokenizeIntoArgv(dbg_string, argv);
}

void LocalExecutor::GetPerfDataFromLine(TaskFinalReport* report,
                                        const string& line) {
  boost::regex e("[[:space:]]*? ([0-9,.]+) ([a-zA-Z-]+) .*");
  boost::smatch m;
  if (boost::regex_match(line, m, e, boost::match_extra)
      && m.size() == 3) {
    string number = m[1].str();
    // Remove any commas
    number.erase(std::remove(number.begin(), number.end(), ','), number.end());
    VLOG(1) << "matched: " << m[2] << ", " << number;
    if (m[2] == "instructions") {
      report->set_instructions(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "cycles") {
      report->set_cycles(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "seconds") {
      report->set_runtime(strtold(number.c_str(), NULL));
    } else if (m[2] == "cache-references") {
      report->set_llc_refs(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "cache-misses") {
      report->set_llc_misses(strtoul(number.c_str(), NULL, 10));
    }
  }
}

void LocalExecutor::HandleTaskCompletion(const TaskDescriptor& td,
                                         TaskFinalReport* report) {
  report->set_task_id(GenerateTaskEquivClass(td));
  // Load perf data, if it exists
  if (FLAGS_perf_monitoring) {
    FILE* fptr;
    char line[300];
    // XXX(malte): This is an ugly hack that avoids a race between the data file
    // being written by the perf utility and it being opened for reading.
    // We really need a proper solution here, especially as this is on the
    // critical path in the coordinator's main event handler thread.
    sleep(1);
    if ((fptr = fopen(PerfDataFileName(td).c_str(), "r")) == NULL) {
      LOG(ERROR) << "Failed to open FD for reading of perf data. FD " << fptr;
    }
    VLOG(1) << "Processing perf output in file " << PerfDataFileName(td)
            << "...";
    while (!feof(fptr)) {
      if (fgets(line, 300, fptr) != NULL) {
        GetPerfDataFromLine(report, line);
      }
    }
    fclose(fptr);
  }
}

void LocalExecutor::RunTask(TaskDescriptor* td,
                            bool firmament_binary) {
  CHECK(td);
  // XXX(malte): Move this over to use RunProcessAsync, instead of custom thread
  // spawning.
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  boost::unique_lock<boost::mutex> lock(exec_mutex_);
  boost::thread per_task_thread(
      boost::bind(&LocalExecutor::_RunTask, this, td, firmament_binary));
  exec_condvar_.wait(lock);
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
  bool res = (RunProcessSync(
      td->binary(), args, (FLAGS_perf_monitoring && firmament_binary),
      (FLAGS_debug_tasks || ((FLAGS_debug_interactively != 0) &&
                             (td->uid() == FLAGS_debug_interactively))),
      firmament_binary) == 0);
  VLOG(1) << "Result of RunProcessSync was " << res;
  return res;
}

int32_t LocalExecutor::RunProcessAsync(const string& cmdline,
                                       vector<string> args,
                                       bool perf_monitoring,
                                       bool debug,
                                       bool default_args) {
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  boost::thread async_process_thread(
      boost::bind(&LocalExecutor::RunProcessSync, this, cmdline, args,
                  perf_monitoring, debug, default_args));
  // We hard-code the return to zero here; maybe should return a thread
  // reference instead.
  return 0;
}

int32_t LocalExecutor::RunProcessSync(const string& cmdline,
                                      vector<string> args,
                                      bool perf_monitoring,
                                      bool debug,
                                      bool default_args) {
  char* perf_prefix;
  pid_t pid;
  /*int pipe_to[2];    // pipe to feed input data to task
  int pipe_from[3];  // pipe to receive output data from task
  if (pipe(pipe_to) != 0) {
    PLOG(ERROR) << "Failed to create pipe to task.";
  }
  if (pipe(pipe_from) != 0) {
    PLOG(ERROR) << "Failed to create pipe from task.";
  }*/
  vector<char*> argv;
  // N.B.: only one of debug and perf_monitoring can be active at a time;
  // debug takes priority here.
  if (debug) {
    // task debugging is active, so reserve extra space for the
    // gdb invocation prefix.
    argv.reserve(args.size() + (default_args ? 4 : 3));
    perf_prefix = AddDebuggingToCommandLine(&argv);
  } else if (perf_monitoring) {
    // performance monitoring is active, so reserve extra space for the
    // "perf" invocation prefix.
    argv.reserve(args.size() + (default_args ? 11 : 10));
    perf_prefix = AddPerfMonitoringToCommandLine(&argv);
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
  LOG(INFO) << "COMMAND LINE for task " << getenv("FLAGS_task_id") << ": "
            << full_cmd_line;
  VLOG(1) << "About to fork child process for task execution of "
          << getenv("FLAGS_task_id") << "!";
  pid = fork();
  switch (pid) {
    case -1:
      // Error
      LOG(ERROR) << "Failed to fork child process.";
      break;
    case 0: {
      // Child
      // set up pipes
      //dup2(pipe_to[0], STDIN_FILENO);
      //dup2(pipe_from[1], STDOUT_FILENO);
      //dup2(pipe_from[2], STDERR_FILENO);
      // close unnecessary pipe descriptors
      //close(pipe_to[0]);
      //close(pipe_to[1]);
      //close(pipe_from[0]);
      //close(pipe_from[1]);
      //close(pipe_from[2]);
      // Convert args from string to char*
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
      //close(pipe_to[0]);
      //close(pipe_from[1]);
      // TODO(malte): fix the pipe stuff to work properly
      //close(pipe_to[1]);
      // TODO(malte): ReadFromPipe is a synchronous call that will only return
      // once the pipe has been closed! Check if this is actually the semantic
      // we want.
      // The fact that we cannot concurrently read from the STDOUT and the
      // STDERR pipe this way suggest the answer is that it is not...
      //ReadFromPipe(pipe_from[0]);
      //ReadFromPipe(pipe_from[1]);
      // Notify any other threads waiting to execute processes (?)
      exec_condvar_.notify_one();
      // Wait for task to terminate
      int status;
      while (waitpid(pid, &status, 0) != pid) {
        VLOG(3) << "Waiting for child process " << pid << " to exit...";
      }
      if (WIFEXITED(status)) {
        VLOG(1) << "Task process with PID " << pid << " exited with status "
                << WEXITSTATUS(status);
      } else if (WIFSIGNALED(status)) {
        VLOG(1) << "Task process with PID " << pid << " exited due to uncaught "
                << "signal " << WTERMSIG(status);
      } else if (WIFSTOPPED(status)) {
        VLOG(1) << "Task process with PID " << pid << " is stopped due to "
                << "signal " << WSTOPSIG(status);
      } else {
        LOG(ERROR) << "Unexpected exit status: " << hex << status;
      }
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
  env.push_back(EnvPair_t("FLAGS_heartbeat_interval",
                          to_string(heartbeat_interval_)));
  // Set environment variables
  VLOG(2) << "Task's environment variables:";
  for (vector<EnvPair_t>::const_iterator env_iter = env.begin();
       env_iter != env.end();
       ++env_iter) {
    VLOG(2) << "  " << env_iter->first << ": " << env_iter->second;
    setenv(env_iter->first.c_str(), env_iter->second.c_str(), 1);
  }
}

char* LocalExecutor::TokenizeIntoArgv(const string& str, vector<char*>* argv) {
  // Ugly parsing code to transform str into an argv representation
  char* str_c_string = (char*)malloc(str.size()+1);  // NOLINT
  snprintf(str_c_string, str.size(), "%s", str.c_str());
  char* piece;
  char* tmp_ptr = NULL;
  piece = strtok_r(str_c_string, " ", &tmp_ptr);
  while (piece != NULL) {
    argv->push_back(piece);
    piece = strtok_r(NULL, " ", &tmp_ptr);
  }
  // Return pointer to the allocated string in order to be able to delete it
  // later.
  VLOG(1) << "After adding tokenized version of '" << str
          << "', size of argv is " << argv->size();
  return str_c_string;
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
