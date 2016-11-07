// The Firmament project
// Copyright (c) The Firmament Authors.
//
// A bridge to TaskLib for use in applications that link TaskLib at
// compilation time.

#ifndef FIRMAMENT_EXAMPLE_TASK_LIB_BRIDGE_H
#define FIRMAMENT_EXAMPLE_TASK_LIB_BRIDGE_H

#include "base/common.h"
#include "base/types.h"

#include <vector>

using std::vector;

// Local task lib instance
firmament::TaskLib* task_lib_;

void TerminationCleanup() {
  if (task_lib_) {
    task_lib_->Stop(true);
  }
}

// Sets up and runs a TaskLib monitor in the current thread.
void LaunchTasklib(int argc, char** argv) {
  sleep(1);

  // Ensure that we get log output from TaskLib
  setenv("GLOG_logtostderr", "1", 1);

  string prog = "task_lib";
  boost::thread::id task_thread_id = boost::this_thread::get_id();

  argv[0] = const_cast<char*>(prog.c_str());

  firmament::common::InitFirmament(argc, argv);

  task_lib_->RunMonitor(task_thread_id);
}

int main(int argc, char** argv) {
  // Unset LD_PRELOAD to avoid us from starting launching monitors in
  // child processes, unless we're in a wrapper
  setenv("LD_PRELOAD", "", 1);

  // Cleanup task lib before terminating the process.
  atexit(TerminationCleanup);

  LOG(INFO) << "Starting TaskLib monitor thread";

  task_lib_ = new firmament::TaskLib();

  boost::thread t1(&LaunchTasklib, argc, argv);

  vector<char*> args;
  for (int64_t i = 1; i < argc; ++i) {
    args.push_back(argv[i]);
  }
  firmament::task_main(0, &args);
}

#endif  // FIRMAMENT_EXAMPLE_TASK_LIB_BRIDGE_H
