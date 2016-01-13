// The Firmament project
// Copyright (c) 2015 Malte Schwarzkopf <malte.schwarzkopf@cl.cam.ac.uk>
//
// Common unit conversion constants.

#ifndef FIRMAMENT_BASE_UNITS_H
#define FIRMAMENT_BASE_UNITS_H

#include <stdint.h>

namespace firmament {

// Capacity
const uint64_t BYTES_TO_MB = 1024 * 1024;
const uint64_t BYTES_TO_GB = 1024 * 1024 * 1024;
const uint64_t KB_TO_MB = 1024;
const uint64_t KB_TO_GB = 1024 * 1024;
const uint64_t MB_TO_BYTES = 1024 * 1024;

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
