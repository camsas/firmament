// TODO: header

#include "sim/event_queue.h"

namespace firmament {

void EventQueue::AddJobEvent(double time, SimEvent::SimEventType type,
                             JobSim *job) {
  event_queue_.push(new SimEvent(time, type, job, NULL));
  VLOG(2) << "Added new job event of type " << type << " for job "
          << job->name() << " at time " << time << " to event queue.";
}

void EventQueue::AddTaskEvent(double time, SimEvent::SimEventType type,
                              JobSim *job, TaskSim *task) {
  event_queue_.push(new SimEvent(time, type, job, task));
  VLOG(2) << "Added new task event of type " << type << " for task "
          << task->name() << " in job " << job->name() << ", to be processed "
          << "at time " << time;
}

SimEvent* EventQueue::GetNextEvent() {
  VLOG(2) << "GetNextEvent called, queue size is " << event_queue_.size()
          << ", first event time is " << event_queue_.top()->time();
  if (event_queue_.size() > 0) {
    SimEvent *tmp = event_queue_.top();
//    event_queue_.pop();
    return tmp;
  } else
    return NULL;
}

void EventQueue::PopEvent() {
  if (event_queue_.size() > 0)
    event_queue_.pop();
}

}  // namespace firmament
