// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>
#ifndef FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
#define FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H

namespace firmament {

class GoogleRuntimeDistribution {
 public:
  GoogleRuntimeDistribution(double factor, double power);

  /**
   * @param x runtime in milliseconds
   * @return proportion of values in distribution <= x
   */
  double distribution(double x);
 private:
  double factor_;
  double power_;
};

} // namespace firmament

#endif // FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
