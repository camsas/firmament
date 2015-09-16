// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#ifndef FIRMAMENT_SIM_DFS_SIMULATED_DFS_H
#define FIRMAMENT_SIM_DFS_SIMULATED_DFS_H

#include <list>
#include <queue>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/common.h"
#include "base/resource_topology_node_desc.pb.h"
#include "base/types.h"
#include "sim/dfs/google_block_distribution.h"

namespace firmament {
namespace sim {
namespace dfs {

typedef unordered_set<ResourceID_t, boost::hash<boost::uuids::uuid> >
        ResourceSet_t;

class SimulatedDFS {
 public:
  typedef uint64_t FileID_t;
  typedef uint32_t NumBlocks_t;

  SimulatedDFS(uint64_t num_machines, NumBlocks_t blocks_per_machine,
               uint32_t replication_factor,
               GoogleBlockDistribution *block_distribution,
               uint64_t random_seed);

  void AddMachine(ResourceID_t machine);
  void RemoveMachine(ResourceID_t machine);

  NumBlocks_t GetNumBlocks(FileID_t file) const {
    return files_[file];
  }
  const ResourceSet_t GetMachines(FileID_t file) const;
  /**
   * Returns a set of files that have uniformly been sampled. The files
   * have consist of num_blocks +- tolerance.
   * @param num_blocks the number of blocks the files are expected to have
   * @param tolerance the tolerance (in percentage)
   */
  unordered_set<FileID_t> SampleFiles(NumBlocks_t num_blocks,
                                      uint32_t tolerance) const;

 private:
  void AddFile();
  uint32_t NumBlocksInFile();

  uint64_t num_blocks_ = 0;
  // pair: start block ID, end block ID (inclusive)
  vector<NumBlocks_t> files_;
  vector<ResourceID_t> machines_;

  uint32_t replication_factor_ = 0;

  mutable default_random_engine generator_;
  uniform_real_distribution<double> uniform_;
  GoogleBlockDistribution *blocks_in_file_distn_;
};

} // namespace dfs
} // namespace sim
} // namespace firmament

#endif // FIRMAMENT_SIM_DFS_SIMULATED_DFS_H
