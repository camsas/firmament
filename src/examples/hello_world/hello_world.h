// The Firmament project
// Copyright (c) The Firmament Authors.
//
// A "hello world" test job.

#ifndef FIRMAMENT_EXAMPLE_HELLO_WORLD_H
#define FIRMAMENT_EXAMPLE_HELLO_WORLD_H

#include "base/task_interface.h"

namespace firmament {
namespace examples {
namespace hello_world {

class HelloWorldTask : public TaskInterface {
 public:
  explicit HelloWorldTask(TaskID_t task_id)
    : TaskInterface(task_id) {
    VLOG(3) << "Constructing HelloWorldTask";
  }
  void Invoke();
};

}  // namespace hello_world
}  // namespace examples
}  // namespace firmament

#endif  // FIRMAMENT_EXAMPLE_HELLO_WORLD_H
