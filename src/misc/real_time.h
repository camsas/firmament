// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//

#ifndef FIRMAMENT_MISC_REAL_TIME_H
#define FIRMAMENT_MISC_REAL_TIME_H

#include "misc/time_interface.h"

namespace firmament {

class RealTime : public TimeInterface {
 public:
  virtual ~RealTime();
  // Returns the current epoch timestamp in µ-seconds as an integer.
  // Uses gettimeofday() under the hood, so does not make any guarantees w.r.t.
  // time zones etc.
  uint64_t GetCurrentTimestamp();
};

} // namespace firmament

#endif // FIRMAMENT_MISC_REAL_TIME_H
