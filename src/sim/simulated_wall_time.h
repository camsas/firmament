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

#ifndef FIRMAMENT_SIM_SIMULATED_WALL_TIME_H
#define FIRMAMENT_SIM_SIMULATED_WALL_TIME_H

#include "misc/time_interface.h"

namespace firmament {
namespace sim {

class SimulatedWallTime : public TimeInterface {
 public:
  SimulatedWallTime();
  virtual ~SimulatedWallTime();

  /**
   * Get the current timestamp of the simulation (in u-sec).
   */
  uint64_t GetCurrentTimestamp();

  /**
   * Overwrites the current timestmap of the simulation.
   */
  void UpdateCurrentTimestamp(uint64_t timestamp);

  /**
   * Updates the current timestamp if it is smaller than the given
   * argument.
   */
  void UpdateCurrentTimestampIfSmaller(uint64_t timestamp);

 private:
  uint64_t current_simulation_timestamp_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_SIMULATED_WALL_TIME_H
