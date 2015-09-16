// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H
#define FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H

#include <cstdint>

namespace firmament {
namespace sim {
namespace dfs {

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

} // namespace dfs
} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H
