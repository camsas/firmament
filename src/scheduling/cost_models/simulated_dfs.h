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
	typedef uint64_t BlockID_t;

	SimulatedDFS(FileID_t num_files, BlockID_t blocks_per_machine);
	virtual ~SimulatedDFS();

	void addMachine(ResourceID_t machine);
	void removeMachine(ResourceID_t machine);

	const std::list<ResourceID_t> &getMachines(BlockID_t block) const;
	const std::unordered_set<FileID_t> sampleFiles(BlockID_t num_blocks,
			                                           uint32_t tolerance) const;
private:
	void addFile();
	uint32_t numBlocksInFile();
	void addFiles(FileID_t num_files);

	BlockID_t blocks_per_machine, num_blocks = 0;
	std::vector<BlockID_t> blocks_priority;
	std::vector<BlockID_t> blocks_normal;
	std::vector<std::list<ResourceID_t>> block_index;;
	std::unordered_map<ResourceID_t, std::list<BlockID_t>,
	                   boost::hash<ResourceID_t>> blocks_on_machine;
	// pair: start block ID, end block ID (inclusive)
	std::vector<std::pair<BlockID_t, BlockID_t>> files;

	mutable std::default_random_engine generator;
	std::uniform_real_distribution<double> uniform;
	GoogleBlockDistribution blocks_in_file_distn;
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_ */
