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
  uint64_t c;
  if (n <= 1) {
    c = n;
    VLOG(1) << "F_" << n << " is " << c;
  } else {
    ConcreteReference r(0);
    // TODO(malte): args!
    FutureReference f_tmpl(0);  // XXX(malte): hack!
    vector<FutureReference> o1(1, f_tmpl);
    vector<FutureReference> o2(1, f_tmpl);
    task_lib_->Spawn(r, NULL, &o1); // f(n-1)
    task_lib_->Spawn(r, NULL, &o2); // f(n-2)
    uint64_t* r0_ptr = static_cast<uint64_t*>(
        task_lib_->GetObjectStart(o1[0].id()));
    uint64_t* r1_ptr = static_cast<uint64_t*>(
        task_lib_->GetObjectStart(o2[0].id()));
    task_lib_->GetObjectEnd(o1[0].id());
    task_lib_->GetObjectEnd(o2[0].id());
    // c = f(n-1) + f(n-2)
    c = *r0_ptr + *r1_ptr;
  }
  // write c to output 1
  // PutObjectStart();
  // PutObjectEnd();
  // Publish(out);
}

}  // namespace fibonacci
}  // namespace examples
}  // namespace firmament
