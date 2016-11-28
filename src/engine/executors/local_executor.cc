/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

// Local executor class.

#include "engine/executors/local_executor.h"

extern "C" {
#include <fcntl.h>
#include <signal.h>
#include <stdio.h>
#ifdef __linux__
#include <sys/prctl.h>
#endif
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
}
#include <boost/regex.hpp>

#include "base/common.h"
#include "base/types.h"
#include "base/units.h"
#include "engine/executors/task_health_checker.h"
#include "misc/utils.h"
#include "misc/map-util.h"

DEFINE_bool(pin_tasks_to_cores, true,
            "Pin tasks to their allocated CPU core when executing.");
DEFINE_bool(debug_tasks, false,
            "Run tasks through a debugger (gdb).");
DEFINE_uint64(debug_interactively, 0,
              "Run this task ID inside an interactive debugger.");
DEFINE_bool(perf_monitoring, true,
            "Enable performance monitoring for tasks executed.");
DEFINE_string(task_lib_dir, "build/engine/",
              "Path where task_lib.a and task_lib_inject.so are.");
DEFINE_string(task_log_dir, "/tmp/firmament-log",
              "Path where task logs will be stored.");
DEFINE_string(task_perf_dir, "/tmp/firmament-perf",
              "Path where tasks' perf logs should be written.");
DEFINE_string(task_data_dir, "/tmp/firmament-data",
              "Path where tasks' perf logs should be written.");
DEFINE_string(perf_event_list,
              "cpu-clock,task-clock,context-switches,cpu-migrations,"
              "page-faults,cycles,instructions,branches,branch-misses,"
              "cache-misses,cache-references,stalled-cycles-frontend,"
              "stalled-cycles-backend,node-loads,node-load-misses",
              "Comma-separated list of perf events to monitor.");

namespace firmament {
namespace executor {

using common::pb_to_vector;

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri,
                             TimeInterface* time_manager)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      health_checker_(&task_handler_threads_, &handler_map_mutex_),
      time_manager_(time_manager),
      topology_manager_(shared_ptr<TopologyManager>()),  // NULL
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "No topology manager passed, so will not bind to resource.";
  CreateDirectories();
}

LocalExecutor::LocalExecutor(ResourceID_t resource_id,
                             const string& coordinator_uri,
                             TimeInterface* time_manager,
                             shared_ptr<TopologyManager> topology_mgr)
    : local_resource_id_(resource_id),
      coordinator_uri_(coordinator_uri),
      health_checker_(&task_handler_threads_, &handler_map_mutex_),
      time_manager_(time_manager),
      topology_manager_(topology_mgr),
      heartbeat_interval_(1000000000ULL) {  // 1 billios nanosec = 1 sec
  VLOG(1) << "Executor for resource " << resource_id << " is up: " << *this;
  VLOG(1) << "Tasks will be bound to the resource by the topology manager"
          << "at " << topology_manager_;
  CreateDirectories();
}

char* LocalExecutor::AddPerfMonitoringToCommandLine(
    const unordered_map<string, string>& env,
    vector<char*>* argv) {
  // Define the string prefix for performance monitoring
  //string perf_string = "perf stat -x, -o ";
  VLOG(2) << "Enabling performance monitoring...";
  string perf_string = "perf stat -o ";
  const string* perf_fname = FindOrNull(env, "PERF_FNAME");
  CHECK_NOTNULL(perf_fname);
  perf_string += *perf_fname;
  perf_string += " -e ";
  perf_string += FLAGS_perf_event_list;
  perf_string += " -- ";
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

bool LocalExecutor::CheckRunningTasksHealth(vector<TaskID_t>* failed_tasks) {
  return health_checker_.Run(failed_tasks);
}

void LocalExecutor::CleanUpCompletedTask(const TaskDescriptor& td) {
  // Drop task handler thread
  boost::unique_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
  boost::unique_lock<boost::shared_mutex> pid_lock(pid_map_mutex_);
  task_handler_threads_.erase(td.uid());
  // Issue a kill to make double-sure that the task has finished
  // XXX(malte): this is a hack!
  pid_t* pid = FindOrNull(task_pids_, td.uid());
  CHECK_NOTNULL(pid);
  int ret = kill(*pid, SIGKILL);
  LOG(INFO) << "kill(2) for task " << td.uid() << " returned " << ret;
  task_pids_.erase(td.uid());
}


void LocalExecutor::CreateDirectories() {
  struct stat st;
  // Task logs (stdout and stderr)
  if (!FLAGS_task_log_dir.empty() &&
      stat(FLAGS_task_log_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_log_dir.c_str(), 0700);
  }
  // Tasks' perf logs
  if (!FLAGS_task_perf_dir.empty() &&
      stat(FLAGS_task_perf_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_perf_dir.c_str(), 0700);
  }
  // Tasks' data directory
  if (!FLAGS_task_data_dir.empty() &&
      stat(FLAGS_task_data_dir.c_str(), &st) == -1) {
    mkdir(FLAGS_task_data_dir.c_str(), 0700);
  }
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
      report->set_runtime(strtod(number.c_str(), NULL));
    } else if (m[2] == "cache-references") {
      report->set_llc_refs(strtoul(number.c_str(), NULL, 10));
    } else if (m[2] == "cache-misses") {
      report->set_llc_misses(strtoul(number.c_str(), NULL, 10));
    }
  }
}

void LocalExecutor::HandleTaskCompletion(TaskDescriptor* td,
                                         TaskFinalReport* report) {
  uint64_t end_time = time_manager_->GetCurrentTimestamp();
  td->set_finish_time(end_time);
  td->set_total_run_time(UpdateTaskTotalRunTime(*td));
  uint64_t start_time = td->start_time();
  report->set_task_id(td->uid());
  report->set_start_time(start_time);
  report->set_finish_time(end_time);
  // Load perf data, if it exists
  if (FLAGS_perf_monitoring) {
    FILE* fptr;
    char line[1024];
    string file_name = PerfDataFileName(*td);
    if (WaitForPerfFile(file_name)) {
      // Once we get here, we have non-zero data in the perf data file
      if ((fptr = fopen(file_name.c_str(), "r")) == NULL) {
        LOG(ERROR) << "Failed to open perf data file " << file_name;
      } else {
        VLOG(1) << "Processing perf output in file " << file_name
                << "...";
        while (!feof(fptr)) {
          char* lptr = fgets(line, 1024, fptr);
          if (lptr != NULL) {
            GetPerfDataFromLine(report, line);
          }
        }
        if (fclose(fptr) != 0) {
          PLOG(ERROR) << "Failed to close perf file " << file_name
                      << " after reading!";
        }
      }
    } else {
      LOG(ERROR) << "Perf file " << file_name << " does not exists or does not "
                 << "contain data!";
    }
  } else {
    // TODO(malte): this is a bit of a hack -- when we don't have the perf
    // information available, we use the executor's runtime measurements.
    // They should be identical, however, so maybe we should just always do
    // this. Multiplication by 1M converts from microseconds to seconds.
    report->set_runtime(end_time / SECONDS_TO_MICROSECONDS -
                        start_time / SECONDS_TO_MICROSECONDS);
  }
  // Now clean up any remaining state.
  CleanUpCompletedTask(*td);
}

void LocalExecutor::HandleTaskEviction(TaskDescriptor* td) {
  td->set_finish_time(time_manager_->GetCurrentTimestamp());
  td->set_total_run_time(UpdateTaskTotalRunTime(*td));
  td->set_submit_time(time_manager_->GetCurrentTimestamp());
  // TODO(ionel): Implement.
}

void LocalExecutor::HandleTaskFailure(TaskDescriptor* td) {
  td->set_finish_time(time_manager_->GetCurrentTimestamp());
  td->set_total_run_time(UpdateTaskTotalRunTime(*td));
  // Nothing to be done other than cleaning up; there is no final
  // report for failed task at this time.
  CleanUpCompletedTask(*td);
}

void LocalExecutor::RunTask(TaskDescriptor* td,
                            bool firmament_binary) {
  CHECK(td);
  // Save the start time.
  uint64_t start_time = time_manager_->GetCurrentTimestamp();
  // Mark the start time of the task.
  td->set_start_time(start_time);
  td->set_total_unscheduled_time(UpdateTaskTotalUnscheduledTime(*td));
  // XXX(malte): Move this over to use RunProcessAsync, instead of custom thread
  // spawning.
  boost::unique_lock<boost::mutex> exec_lock(exec_mutex_);
  boost::unique_lock<boost::shared_mutex> handler_lock(handler_map_mutex_);
  boost::thread* task_thread = new boost::thread(
      boost::bind(&LocalExecutor::_RunTask, this, td, firmament_binary));
  CHECK(InsertIfNotPresent(&task_handler_threads_, td->uid(), task_thread));
  exec_condvar_.wait(exec_lock);
}

bool LocalExecutor::_RunTask(TaskDescriptor* td,
                             bool firmament_binary) {
  // Convert arguments as specified in TD into a string vector that we can munge
  // into an actual argv[].
  vector<string> args;
  unordered_map<string, string> env;
  // Arguments
  if (td->args_size() > 0) {
    args = pb_to_vector(td->args());
  }
  // Environment variables
  SetUpEnvironmentForTask(*td, &env);
  // Path for task log files (stdout/stderr)
  string tasklog = FLAGS_task_log_dir + "/" + td->job_id() +
                   "-" + to_string(td->uid());
  // TODO(malte): This is somewhat hackish
  // arguments: binary (path + name), arguments, performance monitoring on/off,
  // debugging flags, is this a Firmament task binary? (on/off; will cause
  // default arugments to be passed)
  bool res = (RunProcessSync(
      td->uid(), td->binary(), args, env, FLAGS_perf_monitoring,
      (FLAGS_debug_tasks || ((FLAGS_debug_interactively != 0) &&
                             (td->uid() == FLAGS_debug_interactively))),
      firmament_binary, tasklog) == 0);
  VLOG(1) << "Result of RunProcessSync was " << res;
  return res;
}

int32_t LocalExecutor::RunProcessAsync(TaskID_t task_id,
                                       const string& cmdline,
                                       vector<string> args,
                                       unordered_map<string, string> env,
                                       bool perf_monitoring,
                                       bool debug,
                                       bool default_args,
                                       const string& tasklog) {
  // TODO(malte): We lose the thread reference here, so we can never join this
  // thread. Need to return or store if we ever need it for anythign again.
  boost::thread async_process_thread(
      boost::bind(&LocalExecutor::RunProcessSync, this, task_id, cmdline, args,
                  env, perf_monitoring, debug, default_args, tasklog));
  // We hard-code the return to zero here; maybe should return a thread
  // reference instead.
  return 0;
}

int32_t LocalExecutor::RunProcessSync(TaskID_t task_id,
                                      const string& cmdline,
                                      vector<string> args,
                                      unordered_map<string, string> env,
                                      bool perf_monitoring,
                                      bool debug,
                                      bool default_args,
                                      const string& tasklog) {
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
  vector<char*> envv;
  // Get paths for task logs
  string tasklog_stdout = tasklog + "-stdout";
  string tasklog_stderr = tasklog + "-stderr";
  // N.B.: only one of debug and perf_monitoring can be active at a time;
  // debug takes priority here.
  if (debug) {
    // task debugging is active, so reserve extra space for the
    // gdb invocation prefix.
    argv.reserve(args.size() + (default_args ? 4 : 3));
    AddDebuggingToCommandLine(&argv);
  } else if (perf_monitoring) {
    // performance monitoring is active, so reserve extra space for the
    // "perf" invocation prefix.
    argv.reserve(args.size() + (default_args ? 11 : 10));
    AddPerfMonitoringToCommandLine(env, &argv);
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
    // Unsafe, but okay since args lives beyond the execvpe call.
    VLOG(1) << "Adding extra argument \"" << args[i] << "\"";
    argv.push_back((char*)(args[i].c_str()));  // NOLINT
  }
  vector<string> env_strings(env.size());
  uint64_t i = 0;
  for (unordered_map<string, string>::const_iterator it = env.begin();
       it != env.end();
       ++it) {
    env_strings[i] = it->first + "=" + it->second;
    // N.B.: This casts away the const qualifier on the c_str() result.
    // Unsafe, but okay since env_str lives beyond the execvpe call.
    envv.push_back((char*)(env_strings[i].c_str()));  // NOLINT
    ++i;
  }
  // The last item in the argv and envp for execvpe is always NULL.
  argv.push_back(NULL);
  envv.push_back(NULL);
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
  LOG(INFO) << "COMMAND LINE for task " << task_id << ": "
            << full_cmd_line;
  VLOG(1) << "About to fork child process for task execution of "
          << task_id << "!";
  pid = fork();
  switch (pid) {
    case -1:
      // Error
      LOG(ERROR) << "Failed to fork child process.";
      break;
    case 0: {
      // Child
      // Set up stderr and stdout log redirections to files
      int stdout_fd = open(tasklog_stdout.c_str(),
                           O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
      dup2(stdout_fd, STDOUT_FILENO);
      int stderr_fd = open(tasklog_stderr.c_str(),
                           O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
      dup2(stderr_fd, STDERR_FILENO);
      if (close(stdout_fd) != 0)
        PLOG(FATAL) << "Failed to close stdout FD in child";
      if (close(stderr_fd) != 0)
        PLOG(FATAL) << "Failed to close stderr FD in child";

      // Change to task's working directory
      if (!FindOrNull(env, "FLAGS_task_data_dir") &&
          !env["FLAGS_task_data_dir"].empty()) {
        CHECK_EQ(chdir(env["FLAGS_task_data_dir"].c_str()), 0);
      }

      // Close the open FDs in the child before exec-ing, so that the task does
      // not inherit all of the coordinator's sockets and FDs.
      // We start from 3 here in order to avoid closing stdin/stdout/stderr.
      int fd;
      int fds;
      if ((fds = getdtablesize()) == -1) fds = OPEN_MAX_GUESS;
      for (fd = 3; fd < fds; fd++) {
        if (close(fd) == -EBADF)
          break;
      }

      // kill child process if parent terminates
      // SOMEDAY(adam): make this portable beyond Linux?
#ifdef __linux__
      prctl(PR_SET_PDEATHSIG, SIGHUP);
#endif

      // Run the task binary
      execvpe(argv[0], &argv[0], &envv[0]);
      // execl only returns if there was an error
      PLOG(ERROR) << "execvp failed for task command '" << full_cmd_line << "'";
      //ReportTaskExecutionFailure();
      _exit(1);
    }
    default:
      // Parent
      VLOG(1) << "Task process with PID " << pid << " created.";
      {
        boost::unique_lock<boost::shared_mutex> handler_lock(pid_map_mutex_);
        CHECK(InsertIfNotPresent(&task_pids_, task_id, pid));
      }
      // Pin the task to the appropriate resource
      if (topology_manager_ && FLAGS_pin_tasks_to_cores)
        topology_manager_->BindPIDToResource(pid, local_resource_id_);
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
  string fname = FLAGS_task_perf_dir + "/" + (to_string(local_resource_id_)) +
                 "-" + to_string(td.uid()) + ".perf";
  return fname;
}

void LocalExecutor::SetUpEnvironmentForTask(
    const TaskDescriptor& td,
    unordered_map<string, string>* env) {
  if (coordinator_uri_.empty())
    LOG(WARNING) << "Executor does not have the coordinator_uri_ field set. "
                 << "The task will be unable to communicate with the "
                 << "coordinator!";
  // Make data directory for task
  string data_dir = FLAGS_task_data_dir + "/" + td.job_id() + "-" +
                    to_string(td.uid());
  mkdir(data_dir.c_str(), 0700);
  // Set environment variables
  // N.B.: we pass a completely scrubbed environment to the task, so we need to
  // define even things like PATH that would normally be inherited.
  InsertIfNotPresent(env, "PATH",
      "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin");
  InsertIfNotPresent(env, "FLAGS_task_id", to_string(td.uid()));
  InsertIfNotPresent(env, "PERF_FNAME", PerfDataFileName(td));
  InsertIfNotPresent(env, "FLAGS_coordinator_uri", coordinator_uri_);
  InsertIfNotPresent(env, "FLAGS_resource_id", to_string(local_resource_id_));
  InsertIfNotPresent(env, "FLAGS_heartbeat_interval",
                     to_string(heartbeat_interval_));
  InsertIfNotPresent(env, "FLAGS_task_data_dir", data_dir);
  if (td.inject_task_lib()) {
    InsertIfNotPresent(env, "LD_LIBRARY_PATH", FLAGS_task_lib_dir +
                       ":/usr/local/lib/");
    InsertIfNotPresent(env, "LD_PRELOAD", "libtask_lib_inject.so");
    // This effectively does a "basename" on the executable; the TASK_COMM
    // environment variable is used to match the executable for against
    // /proc/self/comm when deciding whether to inject a monitor thread.
    string expected_comm =
      td.binary().substr(td.binary().rfind("/") + 1,
                         td.binary().size() - td.binary().rfind("/") - 1);
    InsertIfNotPresent(env, "TASK_COMM", expected_comm);
  }
  VLOG(1) << "Task's environment variables:";
  for (unordered_map<string, string>::const_iterator env_iter = env->begin();
       env_iter != env->end();
       ++env_iter) {
    VLOG(1) << "  " << env_iter->first << ": " << env_iter->second;
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

bool LocalExecutor::WaitForPerfFile(const string& file_name) {
  // This hack is required to avoid a race between the data file being
  // written by the perf utility and it being opened for reading.
  // Perf creates a zero-byte file when the task starts, but only syncs
  // data to it when it finishes.
  struct stat st;
  bzero(&st, sizeof(struct stat));
  uint64_t timestamp = time_manager_->GetCurrentTimestamp();
  // Wait at most 10s for perf file to become available
  // TODO(malte): this is a bit of a hack to avoid us getting stuck here
  // forever; we will want to move to a saner, locking-based scheme here.
  if (stat(file_name.c_str(), &st) != 0)
    return false;
  while (st.st_size == 0 && time_manager_->GetCurrentTimestamp() <=
         timestamp + 1 * SECONDS_TO_MICROSECONDS) {
    if (stat(file_name.c_str(), &st) != 0) {
      PLOG(ERROR) << "Failed to stat perf data file " << file_name;
      return false;
    }
  }
  if (st.st_size > 0) {
    // The perf file exists and contains non-zero data
    return true;
  } else {
    return false;
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
  CHECK_EQ(fclose(stream), 0);
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
  CHECK_EQ(fclose(stream), 0);
}

}  // namespace executor
}  // namespace firmament
