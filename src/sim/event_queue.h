// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Event queue for discrete event simulation.

#ifndef FIRMAMENT_SIM_EVENT_QUEUE_H
#define FIRMAMENT_SIM_EVENT_QUEUE_H

#include "base/common.h"
#include "sim/events.h"
#include "sim/simulation_event.pb.h"

#include <queue>

namespace firmament {

class EventQueue {
 public:
  EventQueue() : event_queue_(priority_queue<SimulationEvent*, vector<SimulationEvent*>,
                              SimulationEventComparator>(SimulationEventComparator())) {}
  SimulationEvent* GetNextEvent();
  void PopEvent();
 private:
  priority_queue<SimulationEvent*, vector<SimulationEvent*>, SimulationEventComparator> event_queue_;
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENT_QUEUE_H
