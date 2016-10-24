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

// An interface that can be used whenever we require access to the current time.
// By implementing this interface we can  write a simulated time class that can
// be used in the flow scheduler and in the cost models.

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
  virtual void UpdateCurrentTimestamp(uint64_t timestamp) = 0;
};

} // namespace firmament

#endif // FIRMAMENT_MISC_TIME_INTERFACE_H
