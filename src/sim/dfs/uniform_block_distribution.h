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

#ifndef FIRMAMENT_SIM_DFS_UNIFORM_BLOCK_DISTRIBUTION_H
#define FIRMAMENT_SIM_DFS_UNIFORM_BLOCK_DISTRIBUTION_H
#include "sim/dfs/google_block_distribution.h"
#include <cstdint>

namespace firmament {
namespace sim {

class UniformBlockDistribution  : public GoogleBlockDistribution {
 public:
   UniformBlockDistribution();
  uint64_t Inverse(double y);
 private:
  uint64_t offset_;
  uint64_t range;

};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_UNIFORM_BLOCK_DISTRIBUTION_H
