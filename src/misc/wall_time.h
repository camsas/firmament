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
