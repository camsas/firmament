// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive fibonacci series computation.

#include "storage/reference_types.h"
#include "examples/fibonacci/fibonacci.h"

#include <iostream>  // NOLINT
#include <cstdlib>
#include <vector>

namespace firmament {

void task_main(TaskLib* task_lib, TaskID_t task_id,
               vector<char*>* arg_vec) {
  examples::fibonacci::FibonacciTask t(task_lib, task_id);
  VLOG(1) << "Called task_main, starting " << t;
  // extract arguments
  CHECK_GE(arg_vec->size(), 2);
  uint64_t n = atol(arg_vec->at(1));
  t.Invoke(n);
}

namespace examples {
namespace fibonacci {

void FibonacciTask::Invoke(uint64_t n) {
  // a = read input 1
  // b = read input 2
  if (n <= 1) {
    uint64_t c = n;
    VLOG(1) << "F_" << n << " is " << c;
  } else {
    ConcreteReference r(0);
    // TODO(malte): args!
    vector<FutureReference>* o = NULL;
    task_lib_->Spawn(r, o); // f(n-1)
    task_lib_->Spawn(r, o); // f(n-2)
  }
  // write c to output 1
}

}  // namespace fibonacci
}  // namespace examples
}  // namespace firmament
