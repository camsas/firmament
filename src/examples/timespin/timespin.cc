// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
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

namespace firmament {

void task_main(TaskID_t task_id, vector<char*>* arg_vec) {
  int64_t dur;
  if (arg_vec->size() < 2 || atol(arg_vec->at(1)) <= 0)
    dur = 10;
  else
    dur = atol(arg_vec->at(1));
  VLOG(1) << "Task " << task_id << " spinning for " << dur << " seconds!";
  timespin_main(dur);
}

}  // namespace firmament

volatile sig_atomic_t flag = 1;

void catch_timer(int sig) {
  printf("caught timer signal (%d)\n", sig);
  flag = 0;
}

int timespin_main(int secs) {
  struct itimerval timer;
  timer.it_interval.tv_sec = 0;
  timer.it_interval.tv_usec = 0;
  timer.it_value.tv_sec = secs;
  timer.it_value.tv_usec = 0;

  if (signal(SIGALRM, catch_timer)) return -1;

  if (setitimer(ITIMER_REAL, &timer, NULL)) return -1;

  while (flag) { }

  printf("true");
  return 0;
}
