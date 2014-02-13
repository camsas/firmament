// The Firmament project
// Copyright (c) 2014 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Driver logic for simulated clusters.

#ifndef FIRMAMENT_SIM_SIMULATION_DRIVER_H
#define FIRMAMENT_SIM_SIMULATION_DRIVER_H

#include "base/common.h"
#include "sim/event_queue.h"

namespace firmament {
namespace sim {

class SimulationDriver {
 public:
  SimulationDriver();
  void Run();
 private:
};

}  // namespace sim
}  // namespace firmament

#endif  // FIRMAMENT_SIM_SIMULATION_DRIVER_H
