// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Naive fibonacci series computation.

#include "storage/reference_types.h"
#include "examples/store_example/store_example.h"

#include <iostream>  // NOLINT
#include <cstdlib>
#include <cstdio>
#include <vector>

namespace firmament {

void task_main(TaskLib* task_lib, TaskID_t task_id,
               vector<char*>* arg_vec) {
  examples::store::StoreTask t(task_lib, task_id);
  VLOG(1) << "Called task_main, starting " << t;
  // extract arguments
  t.Invoke();
}

namespace examples {
namespace store {

void StoreTask::Invoke() {
  // a = read input 1
  // b = read input 2
  cout << "Cache is " << task_lib_->getCache()->size << endl;
  // cout << "Testing Get Object Start when no object present " << endl;
  // void* read_ptr = task_lib_->GetObjectStart(0);
  // if (read_ptr==NULL) cout <<" Ptr is null " << endl ;

  cout << "Testing Put Object Start " << endl;
  void* write_ptr = task_lib_->PutObjectStart(1234, 10);
  std::memset(write_ptr, 1, 3);
  cout << "Testing Put Object End " << endl;
  task_lib_->PutObjectEnd(1234, 10);
  cout << "Testing Get Object Start when  object present " << endl;
  void* read_ptr2 = task_lib_->GetObjectStart(1234);
  if (read_ptr2 == NULL)
    cout <<" Ptr is null " << endl;
  cout << "Testing Get Object End when object present " << endl;
  task_lib_->GetObjectEnd(1234);
}

}  // namespace store
}  // namespace examples
}  // namespace firmament
