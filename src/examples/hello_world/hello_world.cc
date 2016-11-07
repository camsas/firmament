// The Firmament project
// Copyright (c) The Firmament Authors.
//
// Simple "hello world" task implementation.

#include "examples/hello_world/hello_world.h"
#include "examples/task_lib_bridge.h"

#include <iostream>  // NOLINT
#include <cstdlib>
#include <vector>

namespace firmament {

void task_main(TaskID_t task_id, vector<char*>* arg_vec) {
  LOG(INFO) << "Called task_main, starting ";
  VLOG(3) << "Called task main";
  examples::hello_world::HelloWorldTask t(task_id);
  t.Invoke();
}

namespace examples {
namespace hello_world {

void HelloWorldTask::Invoke() {
  LOG(INFO) << "Hello world (log)!";
  std::cout << "Hello world (stdout)!\n";
  std::cerr << "Hello world (stderr)!\n";
}

}  // namespace hello_world
}  // namespace examples
}  // namespace firmament
