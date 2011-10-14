// TODO: header

#ifndef FIRMAMENT_SIM_EVENT_QUEUE_H
#define FIRMAMENT_SIM_EVENT_QUEUE_H

#include "base/common.h"
#include "sim/job_sim.h"

#include <queue>

namespace firmament {

struct SimEvent {
  enum SimEventType {
    JOB_ARRIVED = 1,
    JOB_COMPLETED = 2,
    JOB_RUNNING = 3,
    TASK_COMPLETED = 4,
  };
  SimEvent(double time, SimEventType type, JobSim *job, TaskSim *task)
      : time_(time), type_(type), job_(job), task_(task) {}
  double time() const { return time_; }
  SimEventType type() const { return type_; }
  JobSim *job() { return job_; }
  TaskSim *task() { return task_; }

  double time_;
  SimEventType type_;
  JobSim *job_;
  TaskSim *task_;
};

class SimEventComparator {
 public:
  bool operator() (const SimEvent *lhs, const SimEvent *rhs) const {
    return (lhs->time() > rhs->time());
  }
};

class EventQueue {
 public:
  EventQueue() : event_queue_(priority_queue<SimEvent*, vector<SimEvent*>,
                              SimEventComparator>(SimEventComparator())) {}
  void AddJobEvent(double time, SimEvent::SimEventType type, JobSim *job);
  void AddTaskEvent(double time, SimEvent::SimEventType type, JobSim *job,
                    TaskSim *task);
  SimEvent *GetNextEvent();
  void PopEvent();
 private:
  priority_queue<SimEvent*, vector<SimEvent*>, SimEventComparator> event_queue_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENT_QUEUE_H
