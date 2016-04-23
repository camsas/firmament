// The Firmament project
// Copyright (c) 2016 Ionel Gog <ionel.gog@cl.cam.ac.uk>

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
