// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Initialization code for the executor library. This will be linked into the
// task binary, and constitutes the entry point for it. After creating an
// task library instance, setting up watcher threads etc., this will delegate to
// the task's Run() method.

#include <sys/prctl.h>
#include <cstdlib>

#include "base/common.h"
#include "engine/task_lib.h"

//DECLARE_string(coordinator_uri);
DECLARE_string(resource_id);
DECLARE_string(cache_name);
extern char **environ;

using namespace firmament;  // NOLINT

TaskLib *task_lib;

void TerminationCleanup() {
  if (task_lib) {
    task_lib->Stop(true);
  }
}

void LaunchTasklib() {
  /* Sets up and runs a TaskLib monitor in the current thread. */
  // Read these important variables from the environment.
  sleep(1);

  string sargs = "--logtostderr";
  string progargs = "task_lib";
  boost::thread::id task_thread_id = boost::this_thread::get_id();

  char* argv[2];
  argv[0] = const_cast<char*>(progargs.c_str());

  argv[1] = const_cast<char*>(sargs.c_str());
    firmament::common::InitFirmament(2, argv);

  // Set process/thread name for debugging
  prctl(PR_SET_NAME,"TaskLibMonitor", 0, 0, 0);

  task_lib = new TaskLib();
  task_lib->RunMonitor(task_thread_id);
}

__attribute__((constructor)) static void task_lib_main() {
  /*
  Launched through the LD_PRELOAD environment variable.
  Starts a new thread to run the TaskLib monitoring and lets
  the main program continue execution in the current thread.
  */

  // Unset LD_PRELOAD to avoid us from starting launching monitors in
  // child processes, unless we're in a wrapper
  setenv("LD_PRELOAD", "", 1);

  // Grab the current process's name via procfs
  FILE* self_comm = fopen("/proc/self/comm", "r");
  char cur_comm[64];
  fgets(cur_comm, 64, self_comm);
  fclose(self_comm);

  //if (strncmp(&cur_comm, getenv("TASKLIB_TARGET_COMM"), 64) == 0) {
    // Cleanup task lib before terminating the process.
    atexit(TerminationCleanup);

    LOG(INFO) << "Starting TaskLib monitor thread for " << cur_comm;
    boost::thread t1(&LaunchTasklib);
  //} else {
  //  LOG(INFO) << "Not injecting TaskLib into " << cur_comm;
  //}
}
