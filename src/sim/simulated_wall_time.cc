/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

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
