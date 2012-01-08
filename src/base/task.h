// TODO(header)

#ifndef FIRMAMENT_BASE_TASK_H
#define FIRMAMENT_BASE_TASK_H

#include "base/common.h"
#include "base/job.h"

//#include "generated/base/task_desc.pb.h"

namespace firmament {

class Job;

class Task {
 public:
  enum TaskState {
    CREATED = 0,
    RUNNING = 1,
    PENDING = 2,
    COMPLETED = 3,
    ASSIGNED = 4,
    MIGRATED = 5,
    UNKNOWN = 6,
  };

  Task(const string& name);
  uint64_t uid() { return task_uid_; }
  uint64_t index_in_job() { return index_in_job_; }
  void set_index_in_job(const uint64_t idx) { index_in_job_ = idx; }
  const string& name() { return name_; }
  void set_name(const string& name) { name_ = name; }
  TaskState state() { return static_cast<TaskState>(state_); }
  void set_state(TaskState state) {
    state_ = static_cast<TaskState>(state);
  }
  Job *job() {
    CHECK_NOTNULL(job_);
    return job_;
  }
  void set_job(Job *job) {
    CHECK_NOTNULL(job);
    job_ = job;
  }

 protected:
  string name_;
  //TaskDescriptor descriptor_;
 private:
  uint64_t task_uid_;  // Set automatically by constructor
  uint64_t index_in_job_;
  TaskState state_;
  Job *job_;
};

}  // namespace firmament

#endif  // FIRMAMENT_BASE_TASK_H
