#include "simulated_dfs.h"
#include "base/common.h"

#include <algorithm>

#include "misc/SpookyV2.h"

namespace firmament {

// justification for block parameters from Chen, et al (2012)
// blocks: 64 MB, max blocks 160 corresponds to 10 GB
SimulatedDFS::SimulatedDFS(uint64_t num_machines, NumBlocks_t blocks_per_machine,
    uint32_t replication_factor, GoogleBlockDistribution *block_distribution,
    uint64_t random_seed) :
    replication_factor(replication_factor), generator(random_seed),
		uniform(0.0,1.0), blocks_in_file_distn(block_distribution) {
  NumBlocks_t total_block_capacity = blocks_per_machine * num_machines;
  total_block_capacity /= replication_factor;
  LOG(INFO) << "Total capacity of " << total_block_capacity << " blocks.";

  // create files until we hit storage limit
  while (num_blocks < total_block_capacity) {
    addFile();
  }
  LOG(INFO) << num_blocks << " blocks used, across "
            << files.size() << " files.";
}

SimulatedDFS::~SimulatedDFS() { }

void SimulatedDFS::addFile() {
	uint32_t file_size = numBlocksInFile();  // in blocks
	num_blocks += file_size;
	files.push_back(file_size);
}

uint32_t SimulatedDFS::numBlocksInFile() {
	double r = uniform(generator);
	return blocks_in_file_distn->inverse(r);
}

void SimulatedDFS::addMachine(ResourceID_t machine) {
  machines.push_back(machine);
}

void SimulatedDFS::removeMachine(ResourceID_t machine) {
  for (auto it = machines.begin(); it != machines.end(); ++it) {
    if (*it == machine) {
      machines.erase(it);
      return;
    }
  }
  // machine not in list of machines
  CHECK(false);
}

const ResourceSet_t SimulatedDFS::getMachines(FileID_t file) const {
  ResourceSet_t res;
  uint32_t num_machines = machines.size();
  for (uint32_t i = 0; i < replication_factor; i++) {
    uint64_t machine_id = SpookyHash::Hash32(&file, sizeof(file), i);
    machine_id *= num_machines; // 32-bit*32-bit fits in 64-bits
    // always produces an answer in range [0,num_machines-1]
    machine_id /= (uint64_t)UINT32_MAX + 1;

    res.insert(machines[machine_id]);
  }
  return res;
}

const std::unordered_set<SimulatedDFS::FileID_t> SimulatedDFS::sampleFiles
          								 (NumBlocks_t target_blocks, uint32_t tolerance) const {
	CHECK_LE(target_blocks, num_blocks);
	NumBlocks_t min_blocks_to_sample = (target_blocks * (100 - tolerance)) / 100;
	min_blocks_to_sample = std::max(min_blocks_to_sample, (NumBlocks_t)1);
	NumBlocks_t max_blocks_to_sample = (target_blocks * (100 + tolerance)) / 100;

	std::unordered_set<SimulatedDFS::FileID_t> sampled_files;

	std::uniform_int_distribution<size_t> distn(0, files.size() - 1);
	NumBlocks_t blocks_sampled = 0;
	while (blocks_sampled < min_blocks_to_sample) {
		size_t index = distn(generator);
		NumBlocks_t blocks_in_file = files[index];

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

		VLOG(2) << "Sampled file " << index << " with " << blocks_in_file << " blocks";
	}

	return sampled_files;
}

} /* namespace firmament */
