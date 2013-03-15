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
  // cout << "Testing Get Object Start when no object present ";
  // void* read_ptr = task_lib_->GetObjectStart(0);
  // if (read_ptr==NULL) cout <<" Ptr is null ";

  cout << "Testing Put Object Start " << endl;
  DataObjectID_t do1("1234");
  void* write_ptr = task_lib_->PutObjectStart(do1, 10);
  std::memset(write_ptr, 1, 3);
  cout << " Write ptr is " << write_ptr << endl;
  cout << " Written value: " << hex
       << *static_cast<uint64_t*>(write_ptr) << endl;
  cout << "Testing Put Object End " << endl;
  task_lib_->PutObjectEnd(do1, 10);
  cout << "Testing Get Object Start when  object present " << endl;
  void* read_ptr2 = task_lib_->GetObjectStart(do1);
  if (read_ptr2 == NULL) {
    cout << " Ptr is null " << endl;
  } else {
    cout << " Ptr is " << read_ptr2 << endl;
    cout << " Value read: " << hex
         << *static_cast<uint64_t*>(read_ptr2) << endl;
  }
  cout << "Testing Get Object End when object present " << endl;
  task_lib_->GetObjectEnd(do1);
}

}  // namespace store
}  // namespace examples
}  // namespace firmament
