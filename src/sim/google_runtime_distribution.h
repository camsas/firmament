/*
 * Firmament
 * Copyright (c) The Firmament Authors.
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 * LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR
 * A PARTICULAR PURPOSE, MERCHANTABLITY OR NON-INFRINGEMENT.
 *
 * See the Apache Version 2.0 License for specific language governing
 * permissions and limitations under the License.
 */

#ifndef FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
#define FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H

#include "base/common.h"

namespace firmament {
namespace sim {

class GoogleRuntimeDistribution {
 public:
  GoogleRuntimeDistribution(double factor, double power);

  /**
   * @param avg_runtime in microseconds
   * @return proportion of values in distribution <= runtime
   */
  double ProportionShorterTasks(uint64_t avg_runtime);
 private:
  double factor_;
  double power_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_GOOGLE_RUNTIME_DISTRIBUTION_H
