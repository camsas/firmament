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

#include "misc/wall_time.h"

namespace firmament {

WallTime::~WallTime() {
}

uint64_t WallTime::GetCurrentTimestamp() {
  struct timeval ts;
  gettimeofday(&ts, NULL);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
    static_cast<uint64_t>(ts.tv_usec);
}

void WallTime::UpdateCurrentTimestamp(uint64_t timestmap) {
  // NO-OP. We can't change the timestamp returned by a real timer.
}

} // namespace firmament
