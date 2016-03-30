// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H
#define FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H

#include <cstdint>

namespace firmament {
namespace sim {

class GoogleBlockDistribution {
 public:
  GoogleBlockDistribution();
  uint64_t Inverse(double y);
 private:
  double percent_min_;
  double coef_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_GOOGLE_BLOCK_DISTRIBUTION_H
