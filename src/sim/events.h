// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Helper functionality for simulation events.

#ifndef FIRMAMENT_SIM_EVENTS_H
#define FIRMAMENT_SIM_EVENTS_H

#include "sim/simulation_event.pb.h"

namespace firmament {

class SimulationEventComparator {
 public:
  bool operator() (const SimulationEvent* lhs, const SimulationEvent* rhs) const {
    return (lhs->time() > rhs->time());
  }
};

}  // namespace firmament

#endif  // FIRMAMENT_SIM_EVENTS_H
