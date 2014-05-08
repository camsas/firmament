// TODO: header

#ifndef FIRMAMENT_SIM_EVENT_QUEUE_H
#define FIRMAMENT_SIM_EVENT_QUEUE_H

#include "base/common.h"
#include "sim/sim_common.h"
#include "sim/job_sim.h"
#include "sim/ensemble_sim.h"

#include <queue>

namespace firmament {

struct SimEvent {
  enum SimEventType {
    JOB_ARRIVED = 1,
    JOB_COMPLETED = 2,
    JOB_RUNNING = 3,
    TASK_COMPLETED = 4,
  };
  SimEvent(double time, SimEventType type, EnsembleSim *ensemble)
      : time_(time), type_(type), ensemble_(ensemble) {}
  double time() const { return time_; }
  SimEventType type() const { return type_; }
  EnsembleSim *ensemble() { return ensemble_; }
  void set_ensemble(EnsembleSim *ens) { ensemble_ = ens; }

  double time_;
  SimEventType type_;
  EnsembleSim *ensemble_;
};

struct SimJobEvent : public SimEvent {
  SimJobEvent(double time, SimEventType type, EnsembleSim *ensemble,
              JobSim *job)
      : SimEvent(time, type, ensemble),
        job_(job) {}
  JobSim *job() { return job_; }

  JobSim *job_;
};

struct SimTaskEvent : public SimJobEvent {
  SimTaskEvent(double time, SimEventType type, EnsembleSim *ensemble,
               JobSim *job, TaskSim *task)
      : SimJobEvent(time, type, ensemble, job),
        task_(task) {}
  TaskSim *task() { return task_; }

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
  void AddJobEvent(double time, SimEvent::SimEventType type, JobSim *job,
                   EnsembleSim *ensemble);
  void AddTaskEvent(double time, SimEvent::SimEventType type, JobSim *job,
                    TaskSim *task, EnsembleSim *ensemble);
  SimEvent *GetNextEvent();
  void PopEvent();
 private:
  priority_queue<SimEvent*, vector<SimEvent*>, SimEventComparator> event_queue_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENT_QUEUE_H
