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

#include "sim/google_runtime_distribution.h"

#include <algorithm>
#include <cmath>

#include "base/units.h"

namespace firmament {
namespace sim {

// Assumptions from Reiss, et al paper
// Figure 2, log-log scale of inverted CDF of job durations
// Production line
// (x0,y0) = (10^-2, 1.8*10^4)
// (x1,y1) = (6*10^2, 10^3)
// From http://en.wikipedia.org/wiki/Log%E2%80%93log_plot
// Slope m = -0.2627
// Constant = 5368.4
// Giving:
// y = 5368.4*x^-0.2627
// But 1.8*10^4 are the max number of jobs:
// actually want probability, so divide everything through by this.
// Also subtract 1, as it's the inverse CDF.
//
// This gives:
// y = 1 - 0.298*x^-0.2627

GoogleRuntimeDistribution::GoogleRuntimeDistribution(double factor,
                                                     double power):
  factor_(factor), power_(power) {
}

double GoogleRuntimeDistribution::ProportionShorterTasks(uint64_t runtime_us) {
  // x is in milliseconds, but distribution was specified in hours
  double runtime = runtime_us / MICROSECONDS_IN_SECOND;
  runtime /= SECONDS_IN_HOUR;
  double y = 1 - factor_ * pow(runtime, power_);
  return std::min(y, 1.0);
}

} // namespace sim
} // namespace firmament
