// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#include "sim/dfs/google_block_distribution.h"

#include <cmath>

namespace firmament {

const static double STEP = 0.01;

GoogleBlockDistribution::GoogleBlockDistribution(uint64_t percent_min,
                                                 uint64_t min_blocks,
                                                 uint64_t max_blocks) {
  percent_min_ = percent_min / 100.0;
  min_blocks_ = min_blocks;
  coef_ = (1 - percent_min_) / log2(max_blocks);
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
    return min_blocks_;
  } else {
    double x = (y - percent_min_) / coef_;
    x = exp2(x);
    return std::round(x);
  }
}

double GoogleBlockDistribution::Mean() {
  double mean = 0;
  mean += percent_min_ * min_blocks_;
  // estimate for rest of tail
  for (double y = percent_min_ + STEP; y <= 1.0; y += STEP) {
    mean += STEP * Inverse(y);
  }
  return mean;
}

} // namespace firmament
