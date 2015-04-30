#ifndef SRC_SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H_
#define SRC_SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H_

#include <cstdint>

namespace firmament {

class GoogleBlockDistribution {
public:
	GoogleBlockDistribution(uint64_t percent_min, uint64_t min_blocks,
			                    uint64_t max_blocks);
	virtual ~GoogleBlockDistribution();
	uint64_t inverse(double y);
private:
	double p_min, coef;
	uint64_t min_blocks;
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_COST_MODELS_GOOGLE_BLOCK_DISTRIBUTION_H_ */
