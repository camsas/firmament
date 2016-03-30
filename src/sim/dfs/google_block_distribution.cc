// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#include "sim/dfs/google_block_distribution.h"

#include <cmath>

#include "base/common.h"
#include "base/units.h"

// The size (in bytes) the blocks have in the distribution.
#define SIZE_OF_BLOCK_IN_DISTRIBUTION 67108864

// Input size distribution. See Evaluation Plan for derivation of defaults.
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
    return std::round(x) * SIZE_OF_BLOCK_IN_DISTRIBUTION;
  }
}

} // namespace sim
} // namespace firmament
