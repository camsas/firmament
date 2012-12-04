// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Simple "hello world" task implementation.

#include "examples/hello_world/hello_world.h"

#include <iostream>  // NOLINT
#include <cstdlib>
#include <vector>

namespace firmament {

void task_main(TaskID_t task_id, vector<char*>* arg_vec) {
  examples::hello_world::HelloWorldTask t(task_id);
  LOG(INFO) << "Called task_main, starting " << t;
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
