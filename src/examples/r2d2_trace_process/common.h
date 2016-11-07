// The Firmament project
// Copyright (c) The Firmament Authors.
// Copyright (c) 2012 Matthew P. Grosvenor  <matthew.grosvenor@cl.cam.ac.uk>
//
// Common data types and definitions for R2D2 packet capture pipeline.

#ifndef FIRMAMENT_EXAMPLE_R2D2_COMMON_H
#define FIRMAMENT_EXAMPLE_R2D2_COMMON_H

#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#define likely(x) __builtin_expect((x),1)  // NOLINT
#define unlikely(x) __builtin_expect((x),0)  // NOLINT

typedef struct {
    uint64_t timestamp;     // Timestamp in 32.32 fixpoint format
    union {
        struct {
            uint32_t length : 16;       // Length of the packet (up to 65kB
                                        // (as per dag card wlen))
            uint32_t dropped: 8;        // Number of dropped packets behind
                                        // this one
            uint32_t type   : 8;        // Type of the packet
        } value_type_dropped_len;
        uint32_t raw_type_dropped_len;  // Raw 32-bit value, so that we can do
                                        // the & ourselves.
    };
    uint64_t hash;          // 64-bit unique identifier for a given packet
} __attribute__((__packed__)) sample_t;


typedef struct {
    uint64_t start_time;
    uint64_t end_time;
    uint64_t samples;
    sample_t first_sample;
} __attribute__((__packed__)) dag_capture_header_t;

// Packet type definitions
enum {
    reserved        = 0x00UL,
    tcp             = 0x01UL,
    udp             = 0x02UL,
    icmp            = 0x03UL,
    arp             = 0x04UL,
    ip_with_opts    = 0x05UL,

    ip_unknown      = 0xDDUL,
    eth_unknown     = 0xEEUL,
    unknown         = 0xFFUL
};

typedef enum {
  RESULT_BEFORE_START = 0,
  RESULT_AFTER_END = 1,
  RESULT_MATCHED = 2,
  RESULT_UNMATCHED = 3
} dag_match_result_t;

#define SECS2NS (1000 * 1000 * 1000)

static inline uint64_t fixed_32_32_to_nanos(uint64_t fixed) {
    uint64_t subsecs = ((fixed & 0xFFFFFFFF) * SECS2NS) >> 32;
    uint64_t seconds = (fixed >> 32) * SECS2NS;
    // Deal with rounding
    if ( 0x4 & fixed ) {
        subsecs += 1;
    }
    if ( (0x2 & fixed) || (0x1 & fixed) ) {
        subsecs += 1;
    }
    return seconds + subsecs;
}

// N.B. prev_timestamp is used for overflow correction here; pass zero to
// avoid it happening.
// The first argument is a capture timestamp in "raw" encoding, the second a
// nanosecond value for the previous timestamp.
static inline uint64_t capture_timestamp_to_nanos_with_overflow_correction(
    uint64_t timestamp, uint64_t prev_timestamp) {
  uint64_t aligned_ts = fixed_32_32_to_nanos(timestamp >> (32 - 5));
  // Overflow correction
  if (prev_timestamp > aligned_ts) {
    aligned_ts += 1 << 5;
  }
  return aligned_ts;
}

static inline uint64_t capture_timestamp_to_nanos(uint64_t timestamp) {
  return capture_timestamp_to_nanos_with_overflow_correction(timestamp, 0);
}

static inline uint64_t secs_from_nanos(uint64_t value) {
  return value / SECS2NS;
}

static inline uint64_t subsecs_from_nanos(uint64_t value) {
  return value - secs_from_nanos(value);
}

static inline uint64_t secs_from_fixed_32_32(uint64_t fixed) {
  return (fixed >> 32);
}

static inline uint64_t subsecs_from_fixed_32_32(uint64_t fixed) {
  return ((fixed & 0xFFFFFFFF) * SECS2NS) >> 32;
}
#ifdef __cplusplus
}  // extern "C"
#endif
#endif  // FIRMAMENT_EXAMPLE_R2D2_COMMON_H
