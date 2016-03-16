// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>
#ifndef FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
#define FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H

namespace firmament {
namespace sim {

class GoogleRuntimeDistribution {
 public:
  GoogleRuntimeDistribution(double factor, double power);

  /**
   * @param runtime in milliseconds
   * @return proportion of values in distribution <= runtime
   */
  double Distribution(double runtime);
 private:
  double factor_;
  double power_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
