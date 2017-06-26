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

#include "sim/dfs/uniform_block_distribution.h"

#include <cmath>

#include "base/common.h"
#include "base/units.h"

// The size (in bytes) the blocks have in the distribution. This value  is the
// block size that must be used in order for the generated distribution to
// match the Facebook input size distribution.
#define SIZE_OF_BLOCK_IN_DISTRIBUTION 67108864

DEFINE_uint64(simulated_quincy_input_min_blocks, 50,
              "Minimum # of blocks in input file");
DEFINE_double(simulated_quincy_input_max_blocks, 150,
              "Maximum # of blocks in input file.");

namespace firmament {
namespace sim {

UniformBlockDistribution::UniformBlockDistribution() {
  offset_ = FLAGS_simulated_quincy_input_min_blocks;
  range_ = FLAGS_simulated_quincy_input_max_blocks - offset_;
}

uint64_t UniformBlockDistribution::Inverse(double y) {
  // Uniform sampling
    uint64_t blocks = offset_ + static_cast<uint64_t>(std::round(y * range_));

    return blocks * SIZE_OF_BLOCK_IN_DISTRIBUTION;
}

} // namespace sim
} // namespace firmament
