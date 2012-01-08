// TODO(header)

#ifndef FIRMAMENT_BASE_JOB_H
#define FIRMAMENT_BASE_JOB_H

#include "base/common.h"
#include "base/task.h"

namespace firmament {

class Task;

class Job {
 public:
  enum JobState {
    RUNNING = 0,
    PENDING = 1,
    COMPLETED = 2,
    FAILED = 3,
    MIGRATED = 4,
    UNKNOWN = 5,
  };

  Job(const string& name);
  const string& name() { return name_; }
  void set_name(const string& name) { name_ = name; }
  JobState state() { return static_cast<JobState>(state_); }
  void set_state(JobState state) {
    state_ = static_cast<JobState>(state);
  }
  uint32_t TaskState(uint64_t task_id) {   // TODO: type
    CHECK_LE(task_id, tasks_.size());
    //return static_cast<Task::TaskState>(tasks_[task_id]->state());
    //return tasks_[task_id]->state();
    return 0;
  }
  bool AddTask(Task *const t);
  uint64_t NumTasks() { return tasks_.size(); }
  uint64_t num_tasks_running() { return num_tasks_running_; }
  void set_num_tasks_running(uint64_t num_tasks) {
    num_tasks_running_ = num_tasks;
  }
  vector<Task*> *GetTasks() { return &tasks_; }

 protected:
  string name_;
  uint64_t job_uid_;  // Set automatically by constructor
  uint32_t state_;
  vector<Task*> tasks_;
  uint64_t num_tasks_running_;
 private:
};

}

#endif  // FIRMAMENT_BASE_JOB_H
