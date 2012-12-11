// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// A test job that (very inefficiently) computes the Fibonacci series.

#ifndef FIRMAMENT_EXAMPLE_FIBONACCI_H
#define FIRMAMENT_EXAMPLE_FIBONACCI_H

#include "base/task_interface.h"

namespace firmament {
namespace examples {
namespace fibonacci {

class FibonacciTask : public TaskInterface {
 public:
  explicit FibonacciTask(TaskLib* task_lib, TaskID_t task_id)
    : TaskInterface(task_lib, task_id) {}
  void Invoke(uint64_t n);
};

}  // namespace fibonacci
}  // namespace examples
}  // namespace firmament

#endif  // FIRMAMENT_EXAMPLE_HELLO_WORLD_H
