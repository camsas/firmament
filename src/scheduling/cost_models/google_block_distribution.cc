#include "google_block_distribution.h"

#include <cmath>

namespace firmament {

GoogleBlockDistribution::GoogleBlockDistribution
              (uint64_t percent_min, uint64_t min_blocks, uint64_t max_blocks) {
	p_min = percent_min / 100.0;
	this->min_blocks = min_blocks;
	coef = (1 - p_min) / log2(max_blocks);
}

GoogleBlockDistribution::~GoogleBlockDistribution() {
	// TODO Auto-generated destructor stub
}

uint64_t GoogleBlockDistribution::inverse(double y) {
	// distribution is F(x) = a + b*lg(x) from Chen
	// we crop it from MIN_NUM_BLOCKS <= x <= MAX_NUM_BLOCKS
	// MIN: justified in the paper, large number of single block jobs
	// MAX: mostly just simplicity
	// a is PROPORTION_MIN
	// b is COEF, computed so that F(MAX_NUM_BLOCKS)=1

	// inverse of this: x = 2^((y-a)/b)
	// sample from this using standard trick of taking U[0,1] and using inverse
	if (y <= p_min) {
		return min_blocks;
	} else {
		double x = (y - p_min) / coef;
		x = exp2(x);
		return std::round(x);
	}
}

} /* namespace firmament */
