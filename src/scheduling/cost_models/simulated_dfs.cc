#include "simulated_dfs.h"
#include "base/common.h"

namespace firmament {

SimulatedDFS::SimulatedDFS() {
	// TODO Auto-generated constructor stub
}

SimulatedDFS::~SimulatedDFS() {
	// TODO Auto-generated destructor stub
}

void SimulatedDFS::addFiles(uint64_t files_to_add) {
	for (uint64_t i = 0; i < files_to_add; i++) {
		addFile();
	}
}

void SimulatedDFS::addFile() {
	// XXX: draw from some distribution
	uint32_t file_size = 10;  // in blocks
	SimulatedDFS::Block_id block_start = num_blocks;
	num_blocks += file_size;
	SimulatedDFS::Block_id block_end = num_blocks - 1;

	SimulatedDFS::File_id file_id = num_files;
	num_files++;

	files[file_id] = std::make_pair(block_start, block_end);
	for (SimulatedDFS::Block_id block = block_start; block <= block_end; block++) {
		replication_factor.push_back(0);
		blocks_priority.push_back(block);
	}
}

std::queue<SimulatedDFS::Block_id>* SimulatedDFS::replicateBlocks
                                     (SimulatedDFS::Block_id num_to_replicate) {
	std::queue<SimulatedDFS::Block_id>* to_replicate =
		                                   new std::queue<SimulatedDFS::Block_id>();

	if (num_blocks < num_to_replicate) {
		LOG(FATAL) << "Requested more blocks " << num_to_replicate
				       << "than in system " << num_blocks;
	}

	while (to_replicate->size() < num_to_replicate) {
		if (blocks_priority.empty()) {
			std::swap(blocks_normal, blocks_priority);
		}
		std::uniform_int_distribution<size_t> distn(0, blocks_priority.size());
		size_t index = distn(generator);

		Block_id block = blocks_priority[index];
		size_t last_index = blocks_priority.size() - 1;
		std::swap(blocks_priority[index], blocks_priority[last_index]);
		blocks_priority.resize(last_index);

	  to_replicate->push(block);
	  blocks_normal.push_back(block);
	  replication_factor[block]++;
	}

	return to_replicate;
}

} /* namespace firmament */
