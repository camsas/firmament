// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H
#define SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H

#include <cstdint>

namespace firmament {

class GoogleBlockDistribution {
 public:
  GoogleBlockDistribution(uint64_t percent_min, uint64_t min_blocks,
                          uint64_t max_blocks);
  uint64_t Inverse(double y);
  double Mean();
 private:
  double percent_min_;
  double coef_;
  uint64_t min_blocks_;
};

} // namespace firmament

#endif /* SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H */
