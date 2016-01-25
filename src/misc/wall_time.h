// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_MISC_WALL_TIME_H
#define FIRMAMENT_MISC_WALL_TIME_H

#include "misc/time_interface.h"

namespace firmament {

class WallTime : public TimeInterface {
 public:
  virtual ~WallTime();
  // Returns the current epoch timestamp in µ-seconds as an integer.
  // Uses gettimeofday() under the hood, so does not make any guarantees w.r.t.
  // time zones etc.
  uint64_t GetCurrentTimestamp();
  void UpdateCurrentTimestamp(uint64_t timestamp);
};

} // namespace firmament

#endif // FIRMAMENT_MISC_WALL_TIME_H
