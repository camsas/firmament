#ifndef SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H
#define SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H

#include <cstdint>

namespace firmament {

class GoogleBlockDistribution {
 public:
  GoogleBlockDistribution(uint64_t percent_min, uint64_t min_blocks,
                          uint64_t max_blocks);
  uint64_t inverse(double y);
  double mean();
 private:
  double p_min, coef;
  uint64_t min_blocks;
};

} // namespace firmament

#endif /* SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H */
