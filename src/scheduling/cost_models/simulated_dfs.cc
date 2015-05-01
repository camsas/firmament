#include "simulated_dfs.h"
#include "base/common.h"

#include <algorithm>

namespace firmament {

// justification for block parameters from Chen, et al (2012)
// blocks: 64 MB, max blocks 160 corresponds to 10 GB
SimulatedDFS::SimulatedDFS(FileID_t num_files, BlockID_t blocks_per_machine) :
		blocks_per_machine(blocks_per_machine),
		uniform(0.0,1.0), blocks_in_file_distn(20, 1, 160) {
	addFiles(num_files);
	if (num_blocks < blocks_per_machine) {
		LOG(FATAL) << "Requested more blocks " << blocks_per_machine
				       << "than in system " << num_blocks;
	}
}

SimulatedDFS::~SimulatedDFS() { }

void SimulatedDFS::addFiles(uint64_t files_to_add) {
	for (uint64_t i = 0; i < files_to_add; i++) {
		addFile();
	}
}

void SimulatedDFS::addFile() {
	uint32_t file_size = numBlocksInFile();  // in blocks
	SimulatedDFS::BlockID_t block_start = num_blocks;
	num_blocks += file_size;
	SimulatedDFS::BlockID_t block_end = num_blocks - 1;

	files.push_back(std::make_pair(block_start, block_end));
	priority_files.push_back(std::make_pair(block_start, block_end));
	for (SimulatedDFS::BlockID_t block = block_start; block <= block_end; block++) {
		block_index.push_back(std::list<ResourceID_t>());
	}
}

uint32_t SimulatedDFS::numBlocksInFile() {
	double r = uniform(generator);
	return blocks_in_file_distn.inverse(r);
}

void SimulatedDFS::addMachine(ResourceID_t machine) {
  std::list<BlockID_t> &local_blocks = blocks_on_machine[machine];
  BlockID_t blocks_to_replicate = blocks_per_machine;
  while (blocks_to_replicate > 0) {
    if (priority_files.empty()) {
      // deep copy of files
      size_t num_files = files.size();
      priority_files.resize(num_files);
      for (size_t i = 0; i < num_files; i++) {
        priority_files[i] = std::pair<BlockID_t, BlockID_t>(files[i]);
      }

      num_replications++;
      LOG(INFO) << "Replication factor reached " << num_replications;
    }

    std::uniform_int_distribution<size_t> distn(0, priority_files.size() - 1);
    size_t index = distn(generator);
    std::pair<BlockID_t, BlockID_t> &block_range = priority_files[index];
    BlockID_t start = block_range.first, end = block_range.second;
    BlockID_t num_blocks_in_file = end - start + 1;

    BlockID_t copy_until = 0;
    if (num_blocks_in_file <= blocks_to_replicate) {
      // replicate entire file
      copy_until = end;

      // file is now as highly replicated as others: delete it from priority
      size_t last_index = priority_files.size() - 1;
      std::swap(priority_files[index], priority_files[last_index]);
      priority_files.resize(last_index);
    } else {
      // only have space to replicate fragment
      copy_until = blocks_to_replicate;

      // some parts of the file maximally replicated, others still priority
      block_range.first = copy_until + 1;
    }
    for (BlockID_t block = start; block <= copy_until; block++) {
      block_index[block].push_back(machine);
      local_blocks.push_back(block);
    }
  }
}

void SimulatedDFS::removeMachine(ResourceID_t machine) {
	std::list<BlockID_t> &local_blocks = blocks_on_machine[machine];
	for (BlockID_t block : local_blocks) {
		block_index[block].remove(machine);;
	}
	blocks_on_machine.erase(machine);
	// SOMEDAY: doesn't update the priority, nor does it attempt to start immediate
	// replication. Would probably be good to randomly select machines to move
	// the orphaned blocks to.
}

const std::pair<SimulatedDFS::BlockID_t, SimulatedDFS::BlockID_t>
                   &SimulatedDFS::getBlocks(SimulatedDFS::FileID_t file) const {
  return files[file];
}

const std::list<ResourceID_t> &SimulatedDFS::getMachines(BlockID_t block) const {
	return block_index[block];
}

const std::unordered_set<SimulatedDFS::FileID_t> SimulatedDFS::sampleFiles
          								 (BlockID_t target_blocks, uint32_t tolerance) const {
	CHECK_LE(target_blocks, num_blocks);
	BlockID_t min_blocks_to_sample = (target_blocks * (100 - tolerance)) / 100;
	min_blocks_to_sample = std::max(min_blocks_to_sample, (unsigned long)1);
	BlockID_t max_blocks_to_sample = (target_blocks * (100 + tolerance)) / 100;

	std::unordered_set<SimulatedDFS::FileID_t> sampled_files;

	std::uniform_int_distribution<size_t> distn(0, files.size() - 1);
	BlockID_t blocks_sampled = 0;
	while (blocks_sampled < min_blocks_to_sample) {
		size_t index = distn(generator);
		std::pair<BlockID_t, BlockID_t> block_range = files[index];
		BlockID_t start = block_range.first, end = block_range.second;
		BlockID_t blocks_in_file = end - start + 1;

		if (sampled_files.count(index) > 0) {
			// already sampled once
			continue;
		}
		if (blocks_sampled + blocks_in_file > max_blocks_to_sample) {
			// file too big
			continue;
		}
		sampled_files.insert(index);
		blocks_sampled += blocks_in_file;
	}

	return sampled_files;
}

} /* namespace firmament */
