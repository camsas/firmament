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
extern char **environ;

using namespace firmament;  // NOLINT

TaskLib* task_lib_;
char self_comm_[64];

void TerminationCleanup() {
  if (task_lib_) {
    task_lib_->Stop(true);
  }
}

void LaunchTasklib() {
  /* Sets up and runs a TaskLib monitor in the current thread. */
  // Read these important variables from the environment.
  sleep(1);

  string progargs = "task_lib";
  boost::thread::id task_thread_id = boost::this_thread::get_id();

  char* argv[1];
  argv[0] = const_cast<char*>(progargs.c_str());

  // Ensure that we get log output from TaskLib
  setenv("GLOG_logtostderr", "1", 1);

  firmament::common::InitFirmament(1, argv);

  // Set process/thread name for debugging
  prctl(PR_SET_NAME, "TaskLibMonitor", 0, 0, 0);

  task_lib_ = new TaskLib();
  task_lib_->RunMonitor(task_thread_id);
}

__attribute__((constructor)) static void task_lib_main() {
  /*
  Launched through the LD_PRELOAD environment variable.
  Starts a new thread to run the TaskLib monitoring and lets
  the main program continue execution in the current thread.
  */

  // Grab the current process's name via procfs
  FILE* comm_fd = fopen("/proc/self/comm", "r");
  CHECK_NOTNULL(comm_fd);
  fgets(self_comm_, 64, comm_fd);
  // The read will have a newline at the end, so replace it
  if (strlen(self_comm_) > 0)
    self_comm_[strlen(self_comm_) - 1] = '\0';
  CHECK_EQ(fclose(comm_fd), 0);

  // Unset LD_PRELOAD to avoid us from starting launching monitors in
  // child processes, unless we're in a wrapper
  char* expected_comm = getenv("TASK_COMM");
  if (self_comm_[0] != '\0' && expected_comm &&
      strcmp(expected_comm, self_comm_) == 0) {
    setenv("LD_PRELOAD", "", 1);
  } else {
    return;
  }

  // Cleanup task lib before terminating the process.
  atexit(TerminationCleanup);

  LOG(INFO) << "Starting TaskLib monitor thread for " << self_comm_;
  boost::thread t1(&LaunchTasklib);
}
