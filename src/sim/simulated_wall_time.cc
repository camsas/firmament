// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#include "sim/simulated_wall_time.h"

DECLARE_uint64(batch_step);

namespace firmament {
namespace sim {

SimulatedWallTime::SimulatedWallTime() : current_simulation_timestamp_(0) {
}

SimulatedWallTime::~SimulatedWallTime() {
}

uint64_t SimulatedWallTime::GetCurrentTimestamp() {
  return current_simulation_timestamp_;
}

void SimulatedWallTime::UpdateCurrentTimestamp(uint64_t timestamp) {
  // In batch mode we run the scheduler every batch_step microseconds. We do
  // not update the timestamp because the runtime of the scheduler is not
  // accounted for in batch mode.
  if (FLAGS_batch_step == 0) {
    // Not running in batch mode => we can update the simulation time.
    current_simulation_timestamp_ = timestamp;
  }
}

void SimulatedWallTime::UpdateCurrentTimestampIfSmaller(uint64_t timestamp) {
  if (current_simulation_timestamp_ < timestamp) {
    current_simulation_timestamp_ = timestamp;
  }
}

} // namespace sim
} // namespace firmament
