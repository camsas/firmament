#ifndef SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_
#define SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_

#include <utility>
#include <unordered_set>
#include <unordered_map>
#include <queue>
#include <list>
#include <random>

#include "base/types.h"
#include "base/resource_topology_node_desc.pb.h"
#include "scheduling/cost_models/google_block_distribution.h"

namespace firmament {

class SimulatedDFS {
public:
	typedef uint64_t FileID_t;
	typedef uint32_t NumBlocks_t;

	SimulatedDFS(uint64_t num_machines, NumBlocks_t blocks_per_machine,
	    uint32_t replication_factor, GoogleBlockDistribution *block_distribution);
	virtual ~SimulatedDFS();

	void addMachine(ResourceID_t machine);
	void removeMachine(ResourceID_t machine);

	NumBlocks_t getNumBlocks(FileID_t file) const {
	  return files[file];
	}
	const std::list<ResourceID_t> getMachines(FileID_t file) const;
	const std::unordered_set<FileID_t> sampleFiles(NumBlocks_t num_blocks,
			                                           uint32_t tolerance) const;

	uint32_t getReplicationFactor() const {
	  return replication_factor;
	}
private:
	void addFile();
	uint32_t numBlocksInFile();

	uint64_t num_blocks = 0;
	// pair: start block ID, end block ID (inclusive)
	std::vector<NumBlocks_t> files;
	std::vector<ResourceID_t> machines;

	uint32_t replication_factor = 0;

	mutable std::default_random_engine generator;
	std::uniform_real_distribution<double> uniform;
	GoogleBlockDistribution *blocks_in_file_distn;
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_ */
