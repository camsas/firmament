// The Firmament project
// Copyright (c) 2015 Adam Gleave <arg58@cam.ac.uk>

#include "sim/dfs/simulated_dfs.h"

#include <algorithm>
#include <SpookyV2.h>

#include "base/common.h"

namespace firmament {

// justification for block parameters from Chen, et al (2012)
// blocks: 64 MB, max blocks 160 corresponds to 10 GB
SimulatedDFS::SimulatedDFS(uint64_t num_machines,
                           NumBlocks_t blocks_per_machine,
                           uint32_t replication_factor,
                           GoogleBlockDistribution *block_distribution,
                           uint64_t random_seed) :
    replication_factor_(replication_factor), generator_(random_seed),
    uniform_(0.0, 1.0), blocks_in_file_distn_(block_distribution) {
  NumBlocks_t total_block_capacity = blocks_per_machine * num_machines;
  total_block_capacity /= replication_factor;
  LOG(INFO) << "Total capacity of " << total_block_capacity << " blocks.";

  // create files until we hit storage limit
  while (num_blocks_ < total_block_capacity) {
    AddFile();
  }
  LOG(INFO) << num_blocks_ << " blocks used, across " << files_.size()
            << " files.";
}

void SimulatedDFS::AddFile() {
  uint32_t file_num_blocks = NumBlocksInFile();
  num_blocks_ += file_num_blocks;
  files_.push_back(file_num_blocks);
}

uint32_t SimulatedDFS::NumBlocksInFile() {
  double r = uniform_(generator_);
  return blocks_in_file_distn_->Inverse(r);
}

void SimulatedDFS::AddMachine(ResourceID_t machine) {
  machines_.push_back(machine);
}

void SimulatedDFS::RemoveMachine(ResourceID_t machine) {
  for (auto it = machines_.begin(); it != machines_.end(); ++it) {
    if (*it == machine) {
      machines_.erase(it);
      return;
    }
  }
  LOG(FATAL) << "Machine is not in list of machines";
}

const ResourceSet_t SimulatedDFS::GetMachines(FileID_t file) const {
  ResourceSet_t res;
  uint32_t num_machines = machines_.size();
  for (uint32_t i = 0; i < replication_factor_; i++) {
    uint64_t machine_id = SpookyHash::Hash32(&file, sizeof(file), i);
    machine_id *= num_machines; // 32-bit*32-bit fits in 64-bits
    // always produces an answer in range [0,num_machines-1]
    machine_id /= (uint64_t)UINT32_MAX + 1;
    res.insert(machines_[machine_id]);
  }
  return res;
}

unordered_set<SimulatedDFS::FileID_t> SimulatedDFS::SampleFiles(
    NumBlocks_t target_blocks, uint32_t tolerance) const {
  CHECK_LE(target_blocks, num_blocks_);
  NumBlocks_t min_blocks_to_sample = (target_blocks * (100 - tolerance)) / 100;
  min_blocks_to_sample = max(min_blocks_to_sample, (NumBlocks_t)1);
  NumBlocks_t max_blocks_to_sample = (target_blocks * (100 + tolerance)) / 100;

  unordered_set<SimulatedDFS::FileID_t> sampled_files;

  uniform_int_distribution<size_t> distn(0, files_.size() - 1);
  NumBlocks_t blocks_sampled = 0;
  while (blocks_sampled < min_blocks_to_sample) {
    size_t index = distn(generator_);
    NumBlocks_t blocks_in_file = files_[index];

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

    VLOG(2) << "Sampled file " << index << " with " << blocks_in_file
            << " blocks";
  }

  return sampled_files;
}

} // namespace firmament
