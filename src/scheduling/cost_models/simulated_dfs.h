#ifndef SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_
#define SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_

#include <utility>
#include <unordered_map>
#include <queue>
#include <random>

namespace firmament {

class SimulatedDFS {
public:
	typedef uint64_t File_id;
	typedef uint64_t Block_id;
	typedef uint32_t Num_replicas;

	SimulatedDFS();
	virtual ~SimulatedDFS();

	void addFiles(File_id num_files);
	std::queue<Block_id>* replicateBlocks(Block_id num_blocks);
private:
	void addFile();

	std::default_random_engine generator;

	File_id num_files = 0;
	Block_id num_blocks = 0;
	std::vector<Block_id> blocks_priority;
	std::vector<Block_id> blocks_normal;
	std::vector<int> replication_factor;
	// pair: start block ID, end block ID (inclusive)
	std::unordered_map<File_id, std::pair<Block_id, Block_id>> files;
};

} /* namespace firmament */

#endif /* SRC_SCHEDULING_COST_MODELS_SIMULATED_DFS_H_ */
