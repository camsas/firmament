// The Firmament project
// Copyright (c) 2011-2012 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// A test job that (very inefficiently) computes the Fibonacci series.

#ifndef FIRMAMENT_EXAMPLE_STORE_H
#define FIRMAMENT_EXAMPLE_STORE_H

#include "base/task_interface.h"

namespace firmament {
namespace examples {
namespace store {

class StoreTask : public TaskInterface {
 public:
  explicit StoreTask(TaskLib* task_lib, TaskID_t task_id)
    : TaskInterface(task_lib, task_id) {}
  void Invoke();
};

}  // namespace store
}  // namespace examples
}  // namespace firmament

#endif  // STORE_EXAMPLE_HELLO_WORLD_H
