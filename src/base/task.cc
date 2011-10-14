// TODO(header)

#include "base/task.h"

namespace firmament {

Task::Task(const string& name)
  : name_(name),
    task_uid_(0),
    state_(Task::CREATED) {
}

}  // namespace firmament
