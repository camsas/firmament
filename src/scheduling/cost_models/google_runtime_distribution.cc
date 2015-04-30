#include "google_runtime_distribution.h"

#include <algorithm>
#include <cmath>

namespace firmament {

// Assumptions from Reiss, et al paper
// Figure 2, log-log scale of inverted CDF of job durations
// Production line
// (x0,y0) = (10^-2, 1.8*10^4)
// (x1,y1) = (6*10^2, 10^3)
// From http://en.wikipedia.org/wiki/Log%E2%80%93log_plot
// Slope m = -0.2627
// Constant = 5368.4
// Giving:
// y = 5368.4*x^-0.2627
// But 1.8*10^4 are the max number of jobs:
// actually want probability, so divide everything through by this.
// Also subtract 1, as it's the inverse CDF.
//
// This gives:
// y = 1 - 0.298*x^-0.2627

GoogleRuntimeDistribution::GoogleRuntimeDistribution(double factor, double power) {
	this->factor = factor;
	this->power = power;
}

GoogleRuntimeDistribution::~GoogleRuntimeDistribution() { }

double GoogleRuntimeDistribution::distribution(double x) {
	// x is in milliseconds, but distribution was specified in hours
	x /= 1000.0; // seconds
	x /= 3600.0; // hours
	double y = 1 - factor*pow(x, power);
	return std::min(y, 1.0);
}

} /* namespace firmament */
