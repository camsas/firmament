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

#include "sim/dfs/google_block_distribution.h"

#include <cmath>

#include "base/common.h"
#include "base/units.h"

// The size (in bytes) the blocks have in the distribution. This value  is the
// block size that must be used in order for the generated distribution to
// match the Facebook input size distribution.
#define SIZE_OF_BLOCK_IN_DISTRIBUTION 67108864

DEFINE_uint64(simulated_quincy_input_percent_min, 50,
              "Percentage of input files which are minimum # of blocks.");
DEFINE_double(simulated_quincy_input_max_blocks, 320,
              "Maximum # of blocks in input file.");

namespace firmament {
namespace sim {

GoogleBlockDistribution::GoogleBlockDistribution() {
  percent_min_ = FLAGS_simulated_quincy_input_percent_min / 100.0;
  coef_ = (1 - percent_min_) / log2(FLAGS_simulated_quincy_input_max_blocks);
}

uint64_t GoogleBlockDistribution::Inverse(double y) {
  // distribution is F(x) = a + b*lg(x) from Chen
  // we crop it from MIN_NUM_BLOCKS <= x <= MAX_NUM_BLOCKS
  // MIN: justified in the paper, large number of single block jobs
  // MAX: mostly just simplicity
  // a is PROPORTION_MIN
  // b is COEF, computed so that F(MAX_NUM_BLOCKS)=1

  // inverse of this: x = 2^((y-a)/b)
  // sample from this using standard trick of taking U[0,1] and using inverse
  if (y <= percent_min_) {
    return SIZE_OF_BLOCK_IN_DISTRIBUTION;
  } else {
    double x = (y - percent_min_) / coef_;
    x = exp2(x);
    return static_cast<uint64_t>(std::round(x)) * SIZE_OF_BLOCK_IN_DISTRIBUTION;
  }
}

} // namespace sim
} // namespace firmament
