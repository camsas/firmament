// The Firmament project
// Copyright (c) 2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Adaptation of Derek Murray's timespin micro-benchmark for Firmament.

/**
 * Simple script that busy-waits until a timer expires, with duration
 * specified (in seconds) on argv[1].
 *
 * Copyright (c) 2011 Derek Murray <Derek.Murray@cl.cam.ac.uk>
 * Copyright (c) 2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "examples/timespin/timespin.h"

#include <vector>

#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <signal.h>

using std::vector;

firmament::TaskLib* task_lib_;

void TerminationCleanup() {
  if (task_lib_) {
    task_lib_->Stop(true);
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

  boost::thread t1(&LaunchTasklib);

  vector<char*> args;
  for (int64_t i = 1; i < argc; ++i) {
    args.push_back(argv[i]);
  }
  firmament::task_main(0, &args);
}

namespace firmament {

void task_main(TaskID_t task_id, vector<char*>* arg_vec) {
  int64_t dur_sec, dur_usec;
  if (arg_vec->size() < 2 ||
      (atol(arg_vec->at(1)) <= 0 && atol(arg_vec->at(2)) <= 0)) {
    dur_sec = 10;
    dur_usec = 0;
  } else {
    dur_sec = atol(arg_vec->at(1));
    dur_usec = atol(arg_vec->at(2));
  }
  VLOG(1) << "Task " << task_id << " spinning for " << dur_sec << " sec, "
          << dur_usec << " µsec!";
  timespin_main(dur_sec, dur_usec);
}

}  // namespace firmament

volatile sig_atomic_t flag = 1;

void catch_timer(int sig) {
  printf("caught timer signal (%d)\n", sig);
  flag = 0;
}

int timespin_main(int secs, int usecs) {
  struct itimerval timer;
  timer.it_interval.tv_sec = 0;
  timer.it_interval.tv_usec = 0;
  timer.it_value.tv_sec = secs;
  timer.it_value.tv_usec = usecs;

  if (signal(SIGALRM, catch_timer)) return -1;

  if (setitimer(ITIMER_REAL, &timer, NULL)) return -1;

  while (flag) { }

  printf("true");
  return 0;
}
