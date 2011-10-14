// TODO(header)

#include "base/job.h"

namespace firmament {

Job::Job(const string& name) 
  : name_(name) {
}

bool Job::AddTask(Task *const t) {
  tasks_.push_back(t);
  return true;
}

}  // namespace firmament
