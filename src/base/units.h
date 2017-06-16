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

// Common unit conversion constants.

#ifndef FIRMAMENT_BASE_UNITS_H
#define FIRMAMENT_BASE_UNITS_H

#include <stdint.h>

namespace firmament {

// Capacity
const uint64_t BYTES_TO_MB = 1024 * 1024;
const uint64_t BYTES_TO_GB = 1024 * 1024 * 1024;
const uint64_t BYTES_TO_KB = 1024;
const uint64_t KB_TO_BYTES = 1024;
const uint64_t KB_TO_MB = 1024;
const uint64_t KB_TO_GB = 1024 * 1024;
const uint64_t MB_TO_BYTES = 1024 * 1024;
const uint64_t MB_TO_KB = 1024;

// Bandwidth
const uint64_t BYTES_TO_MBITS = 8 * 1000 * 1000;
const uint64_t BYTES_TO_GBITS = 8 * 1000 * 1000 * 1000ULL;

// Time
const uint64_t NANOSECONDS_IN_SECOND = 1000 * 1000 * 1000;
const uint64_t NANOSECONDS_IN_MICROSECOND = 1000;
const uint64_t MICROSECONDS_IN_SECOND = 1000 * 1000;
const uint64_t MILLISECONDS_IN_SECOND = 1000;
const uint64_t SECONDS_IN_HOUR = 3600;
const uint64_t MILLISECONDS_TO_MICROSECONDS = 1000;
const uint64_t SECONDS_TO_MILLISECONDS = 1000;
const uint64_t SECONDS_TO_MICROSECONDS = 1000 * 1000;
const uint64_t SECONDS_TO_NANOSECONDS = 1000 * 1000 * 1000;
const uint64_t SECONDS_TO_PICOSECONDS = 1000 * 1000 * 1000 * 1000ULL;

}  // namespace firmament

#endif  // FIRMAMENT_BASE_UNITS_H
