// TODO: header

#include "sim/event_queue.h"

namespace firmament {

SimulationEvent* EventQueue::GetNextEvent() {
  VLOG(2) << "GetNextEvent called, queue size is " << event_queue_.size()
          << ", first event time is " << event_queue_.top()->time();
  if (event_queue_.size() > 0) {
    SimulationEvent* tmp = event_queue_.top();
    return tmp;
  } else {
    return NULL;
  }
}

void EventQueue::PopEvent() {
  if (event_queue_.size() > 0) {
    SimulationEvent* tmp = event_queue_.top();
    event_queue_.pop();
    delete tmp;
  }
}

}  // namespace firmament
