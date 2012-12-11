// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive fibonacci series computation.

#include "examples/fibonacci/fibonacci.h"

#include <iostream>  // NOLINT
#include <cstdlib>

namespace firmament {

void task_main(TaskID_t task_id, Cache_t* cache) {
  examples::fibonacci::FibonacciTask t(task_id, cache);
  VLOG(1) << "Called task_main, starting " << t;
  t.Invoke();
}

namespace examples {
namespace fibonacci {

void FibonacciTask::Invoke() {
  // return ...
}

}  // namespace fibonacci
}  // namespace examples
}  // namespace firmament
