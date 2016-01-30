// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

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
