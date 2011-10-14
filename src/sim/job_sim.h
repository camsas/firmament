// TODO: header

#ifndef FIRMAMENT_SIM_JOB_SIM_H
#define FIRMAMENT_SIM_JOB_SIM_H

#include "base/job.h"
#include "sim/task_sim.h"

namespace firmament {

class JobSim : public Job {
 public:
  JobSim(const string& name, uint64_t num_tasks);
  vector<TaskSim*> *GetTasks() { return &tasks_; }
  uint64_t NumTasks() { return tasks_.size(); }
  bool AddTask(TaskSim *const t);

  double start_time() const {
//    CHECK_GE(start_time_, 0);
    return start_time_;
  }
  double finish_time() const {
//    CHECK_GE(finish_time_, 0);
    return finish_time_;
  }
  double runtime() const {
    CHECK_GE(finish_time_, start_time_);
    return finish_time_ - start_time_;
  }
  void set_start_time(double time) {
    CHECK_GE(time, 0);
    start_time_ = time;
  }
  void set_finish_time(double time) {
    CHECK_GE(time, 0);
    finish_time_ = time;
  }
 private:
  vector<TaskSim*> tasks_;
  double start_time_;
  double finish_time_;
};

}

#endif  // FIRMAMENT_SIM_JOB_SIM_H
