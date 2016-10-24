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

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_SKEWED_DFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_SKEWED_DFS_H

#include "sim/dfs/simulated_uniform_dfs.h"


#include <boost/random/uniform_real_distribution.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/math/distributions/pareto.hpp>

namespace firmament {
namespace sim {

class SimulatedSkewedDFS : public SimulatedUniformDFS {
 public:
  SimulatedSkewedDFS(TraceGenerator* trace_generator);
  ~SimulatedSkewedDFS();

  void AddBlocksForTask(const TaskDescriptor& td, uint64_t num_blocks,
                        uint64_t max_machine_spread);
 private:
  ResourceID_t GetMachineForNewBlock();

  boost::math::pareto_distribution<> pareto_dist_;
  boost::mt19937 rand_gen_;
  boost::random::uniform_real_distribution<> uniform_real_;
  boost::variate_generator<boost::mt19937&,
    boost::random::uniform_real_distribution<> > generator_;
};

} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_SKEWED_DFS_H
