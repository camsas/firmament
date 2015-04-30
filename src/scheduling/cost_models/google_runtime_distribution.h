#ifndef SRC_SCHEDULING_COST_MODELS_GOOGLE_RUNTIME_DISTRIBUTION_H_
#define SRC_SCHEDULING_COST_MODELS_GOOGLE_RUNTIME_DISTRIBUTION_H_

namespace firmament {

class GoogleRuntimeDistribution {
public:
	GoogleRuntimeDistribution(double factor, double power);
	virtual ~GoogleRuntimeDistribution();

	double distribution(double x);
private:
	double factor, power;
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_COST_MODELS_GOOGLE_RUNTIME_DISTRIBUTION_H_ */
