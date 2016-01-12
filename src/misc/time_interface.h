// The Firmament project
// Copyright (c) 2016-2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>
//
// An interface that can be used whenever flow solver requires
// access to the current time. By using this interface we can
// implement a simulation time class that can be used in
// the flow solver and in the cost models.

#ifndef FIRMAMENT_MISC_TIME_INTERFACE_H
#define FIRMAMENT_MISC_TIME_INTERFACE_H

// N.B.: C header for gettimeofday()
extern "C" {
#include <sys/time.h>
}

#include "base/common.h"

namespace firmament {

class TimeInterface {
 public:
  virtual uint64_t GetCurrentTimestamp() = 0;
};

} // namespace firmament

#endif // FIRMAMENT_MISC_TIME_INTERFACE_H
